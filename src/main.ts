import {sleep} from "./util/utils.js";
import {createLogger} from "./util/logger.js";
import pg from 'pg';
import { exec } from "child_process";

const Pool = pg.Pool;
import PQueue from "p-queue";
import Scraper from "./scraper.js";
import {readFileSync} from 'fs'
import {ScraperConfig} from "./types/configs.js";

const configFile = new URL('../config.json', import.meta.url);
const config: ScraperConfig = JSON.parse(readFileSync(configFile, 'utf-8'))

const logger = createLogger('NFTScraper main');

const queueConcurrency = config.queueConcurrency || 16;
const queue = new PQueue({concurrency: queueConcurrency});

const pool = new Pool({
        database: config.dbName,
        user: config.dbUser,
        password: config.dbPass,
        host: config.dbHost,
        port: config.dbPort,
    })

const query = `SELECT *
   FROM nfts
   WHERE (image_cache = '' OR image_cache IS NULL)
     AND metadata IS NOT NULL
     AND metadata::text != '"___INVALID_METADATA___"'::text
     AND (
           scrub_count < 10
           OR scrub_last < NOW() - INTERVAL '1 minutes' AND scrub_count < 40
           OR scrub_last < NOW() - INTERVAL '5 minutes' AND scrub_count < 60
           OR scrub_last < NOW() - INTERVAL '1 hours' AND scrub_count < 80
           OR scrub_last < NOW() - INTERVAL '48 hours' AND scrub_count < 100
           OR scrub_last < NOW() - INTERVAL '384 hours' AND scrub_count < 150
     )
     ORDER BY scrub_last ASC NULLS FIRST
     LIMIT ${config.querySize || 50}
`;

const getCIDStr = (field: any) : string => {
    let ipfsCID = field.match(/^(Qm[1-9A-HJ-NP-Za-km-z]{44,}|b[A-Za-z2-7]{58,}|B[A-Z2-7]{58,}|z[1-9A-HJ-NP-Za-km-z]{48,}|F[0-9A-F]{50,})$/);
    if(ipfsCID !== null){
        let path = ipfsCID[0] + "/";
        let cidParts = field.split(path);
        return (cidParts.length > 1) ? ipfsCID[0] + "/" + cidParts[cidParts.length - 1] : ipfsCID[0];
    }
    return '';
}
const pinCID = (row: NFT) => {
    let ipfsCIDStr = '';
    if(row.metadata?.image){
        if(typeof row.metadata.image === 'string'){
            ipfsCIDStr = getCIDStr(row.metadata.image);
        } else if(row.metadata.image.description) {
            ipfsCIDStr = getCIDStr(row.metadata.image.description);
        }
    } else if(row.metadata?.properties?.image){
        if(typeof row.metadata.properties.image === 'string'){
            ipfsCIDStr = getCIDStr(row.metadata.properties.image);
        } else if (row.metadata.properties.image.description) {
            ipfsCIDStr = getCIDStr(row.metadata.properties.image.description);
        }
    }
    if(ipfsCIDStr !== ''){
        exec("export IPFS_PATH=/ipfs", (err) => {
            if(err){
                console.error("Could not set export path: " + err);
            } else {
                exec("ipfs pin add " + ipfsCIDStr, (e) => {
                    if(e){
                        console.error("Could not pin content with CID "  + ipfsCIDStr + ": " +  e);
                    }
                });
            }
        });
    }   
}
const fillQueue = async () => {
        const {rows} = await pool.query<NFT>(query);
        for (const row of rows) {
            try {
                pinCID(row);
            } catch (e) {
                logger.error(`Exception while pinning NFT: ${e} \n\n ${JSON.stringify(row, null, 4)}`)
            }
            try {
                logger.info(`Scraping ${row.contract}:${row.token_id}`)

                // TODO: Ensure that new NFTs will have last_scrub as NULL and set higher priority for those
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

