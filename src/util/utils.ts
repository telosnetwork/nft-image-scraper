
import { exec } from "child_process";
import {ScraperConfig} from "../types/configs.js";
import {createLogger} from "./logger.js";
const logger = createLogger('database.ts');

export function sleep(ms: number) : Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
}

export function getCIDStr (field: string) : string {
    let ipfsCID = field.match(/^(Qm[1-9A-HJ-NP-Za-km-z]{44,}|b[A-Za-z2-7]{58,}|B[A-Z2-7]{58,}|z[1-9A-HJ-NP-Za-km-z]{48,}|F[0-9A-F]{50,})$/);
    if(ipfsCID !== null){
        let path = ipfsCID[0] + "/";
        let cidParts = field.split(path);
        return (cidParts.length > 1) ? path + cidParts[cidParts.length - 1] : ipfsCID[0];
    }
    return '';
}
export function pinCID(row: NFT) : void {
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
        try {
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
        } catch(e: Error | any){
            logger.error(`Exception while pinning NFT: ${e} \n\n ${JSON.stringify(row, null, 4)}`)
        }
    }   
}


export function getPath(config: ScraperConfig, contract: string, token_id: string, local: boolean = false) : string {
    if(local){
        return `${config.rootDir}/${contract}/${token_id}`
    }
    return `${config.rootUrl}/${contract}/${token_id}`
}
