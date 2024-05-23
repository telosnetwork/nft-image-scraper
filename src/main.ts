import {sleep, getPool, pinCID} from "./util/utils.js";
import {createLogger} from "./util/logger.js";
import { exec, ExecException } from "child_process";

import PQueue from "p-queue";
import Scraper from "./scraper.js";
import {readFileSync} from 'fs'
import {ScraperConfig} from "./types/configs.js";
import { Pool, QueryResult } from "pg";

const configFile = new URL('../config.json', import.meta.url);
const config: ScraperConfig = JSON.parse(readFileSync(configFile, 'utf-8'))

const logger = createLogger('NFTScraper main', config.logLevel);

const queueConcurrency = config.queueConcurrency || 16;
const queue = new PQueue({concurrency: queueConcurrency});

const query = `SELECT *
   FROM nfts
   WHERE (image_cache = '' OR image_cache IS NULL)
     AND metadata IS NOT NULL
     AND metadata::text != '"___INVALID_METADATA___"'::text
     AND (
           scrub_count < 10
           OR scrub_last < NOW() - INTERVAL '5 minutes' AND scrub_count < 20
           OR scrub_last < NOW() - INTERVAL '30 minutes' AND scrub_count < 40
           OR scrub_last < NOW() - INTERVAL '1 hours' AND scrub_count < 50
           OR scrub_last < NOW() - INTERVAL '48 hours' AND scrub_count < 60
           OR scrub_last < NOW() - INTERVAL '96 hours' AND scrub_count < 80
           OR scrub_last < NOW() - INTERVAL '240 hours' AND scrub_count < 100
     )
     ORDER BY scrub_last ASC NULLS FIRST
     LIMIT ${config.querySize || 50}
`;
const fillQueue = async () => {
    for(const i in config.databases){
        const database = config.databases[i];
        logger.debug(`Filling queue for ${database.name}...`);
        let data: QueryResult<NFT> | undefined;
        let pool: Pool;
        try {
            pool = getPool(database);
            data = await pool.query<NFT>(query);
            if(data.rows.length === 0){
                logger.debug(`No data to process found in ${database.name}`);
                continue;
            }
        } catch(e) {
            logger.error(`Error querying database ${database.name}: ${e}`);
            continue;
        }
        for (const row of data?.rows) {
            // TODO: if scrub_count is 99 add as failed in scrapper DB, will get overwritten if following scrap is successful
            // TODO: check if collection + tokenId in database, if so skip next and add indexer DB update in queue using reconstructed URL
            try {
                logger.info(`Scraping ${row.contract}:${row.token_id}`);
                // We get the CID from the metadata and pin it
                let ipfsCID = row.metadata?.image?.match(/^(Qm[1-9A-HJ-NP-Za-km-z]{44,}|b[A-Za-z2-7]{58,}|B[A-Z2-7]{58,}|z[1-9A-HJ-NP-Za-km-z]{48,}|F[0-9A-F]{50,})$/);
                if(ipfsCID !== null){
                    let path = ipfsCID[0] + "/";
                    let cidParts = row.metadata?.image.split(path);
                    let ipfsCIDStr = (cidParts.length > 1) ? path + cidParts[cidParts.length - 1] : ipfsCID[0];
                    logger.info("Pinning " + ipfsCIDStr);
                    exec("export IPFS_PATH=/ipfs", (err : ExecException | null) => {
                        if(err){
                            console.error("Could not set export path: " + err);
                        } else {
                            exec("ipfs pin add " + ipfsCIDStr, (e : ExecException | null) => {
                                if(e){
                                    console.error("Could not pin content with CID "  + ipfsCIDStr + ": " +  e);
                                }
                            });
                        }
                    });
                }
                pinCID(row);
            } catch (e: Error | any) {
                logger.error(`Exception while pinning NFT: ${e} \n\n ${JSON.stringify(row, null, 4)}`)
            }
            try {
                logger.info(`Scraping ${row.contract}:${row.token_id}`)

                // TODO: queue indexer database update if image exists in scrapper database
                queue.add(async () => {
                    try {
                        const scraper = new Scraper(pool, row, config);
                        await scraper.scrapeAndResize();
                    } catch (e: Error | any) {
                        logger.error(`Error running scraper: ${e.message}`)
                    }
                })
                logger.info(`Scraping ${row.contract}:${row.token_id} complete`)
            } catch (e) {
                logger.error(`Exception while scraping NFT: ${e} \n\n ${JSON.stringify(row, null, 4)}`)
            }
        }
    }
}

;(async () => {
    while (true) {
        logger.debug(`Filling queue...`)
        await fillQueue();
        logger.debug(`Sleeping...`)
        await sleep(5000);
        logger.debug(`Done sleeping, size of queue is ${queue.size}`)
        await queue.onSizeLessThan(queueConcurrency)
        logger.debug(`Queue size less than ${queueConcurrency}`)
    }
})().catch((e) => {
    logger.error(`Error while running scraper: ${e.message}`);
})

