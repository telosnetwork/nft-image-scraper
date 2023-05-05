import {sleep} from "./util/utils.js";
import {createLogger} from "./util/logger.js";
import pg from 'pg';

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
           OR scrub_last < NOW() - INTERVAL '1 minutes' AND scrub_count < 50
           OR scrub_last < NOW() - INTERVAL '5 minutes' AND scrub_count < 80
           OR scrub_last < NOW() - INTERVAL '45 minutes' AND scrub_count < 100
     )
     ORDER BY scrub_last ASC NULLS FIRST
     LIMIT ${config.querySize || 50}
`;


const fillQueue = async () => {
        const {rows} = await pool.query<NFT>(query);
        for (const row of rows) {
            try {
                logger.info(`Scraping ${row.contract}:${row.token_id}`)
                
                let ipfsCID = row.metadata?.image?.match(/^(Qm[1-9A-HJ-NP-Za-km-z]{44,}|b[A-Za-z2-7]{58,}|B[A-Z2-7]{58,}|z[1-9A-HJ-NP-Za-km-z]{48,}|F[0-9A-F]{50,})$/);
                if(ipfsCID !== null){
                    exec("export IPFS_PATH=/ipfs", (err) => {
                        if(err){
                            console.error("Could not set export path: " + err);
                        } else {
                            exec("ipfs pin add " + ipfsCID, (e) => {
                                if(e){
                                    console.error("Could not pin content with CID "  + ipfsCID + ": " +  e);
                                }
                            });
                        }
                    });
                }

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
                logger.error(`Exception while scraping NFT: ${JSON.stringify(row, null, 4)}`)
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

