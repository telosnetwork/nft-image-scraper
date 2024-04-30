import {createLogger} from "./logger.js";
import pg from 'pg';
const Pool = pg.Pool;
import { exec } from "child_process";

const logger = createLogger('utils.ts')

export function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms))
}

export function getPool (database: any) {
    return new Pool({
        database: database.name,
        user: database.user,
        password: database.password,
        host: database.host,
        port: database.port,
    })
}
export function getCIDStr (field: any) : string {
    let ipfsCID = field.match(/^(Qm[1-9A-HJ-NP-Za-km-z]{44,}|b[A-Za-z2-7]{58,}|B[A-Z2-7]{58,}|z[1-9A-HJ-NP-Za-km-z]{48,}|F[0-9A-F]{50,})$/);
    if(ipfsCID !== null){
        let path = ipfsCID[0] + "/";
        let cidParts = field.split(path);
        return (cidParts.length > 1) ? path + cidParts[cidParts.length - 1] : ipfsCID[0];
    }
    return '';
}
export function pinCID(row: NFT){
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
