import {Pool} from "pg";
import sharp from 'sharp';
import got from 'got';
import fs from "fs";
import {ScraperConfig} from "./types/configs.js";
import stream from 'node:stream';
import {promisify} from 'node:util'
import { getPath } from "./util/utils.js";
import { Logger } from "pino";
import { getPool } from "./util/database.js";

const pipeline = promisify(stream.pipeline);

const gateways = [
    "https://gateway.pinata.cloud/ipfs/", "https://nftstorage.link/ipfs/", "https://kitchen.mypinata.cloud/ipfs/"
]
const gatewayDomains = [
    "nftstorage.link"
]

export default class Scraper {
    private readonly targetPath: string;
    private cacheUrl: string;
    private imageProperty?: string | null;
    private tmpFile: string;

    constructor(private pool: Pool, private nft: NFT, private config: ScraperConfig,  private logger : Logger) {
        this.targetPath = getPath(config, this.nft.contract, this.nft.token_id, true)
        this.cacheUrl = `${config.rootUrl}/${this.nft.contract}/${this.nft.token_id}`
        this.tmpFile = `${config.tempDir}/${this.nft.contract}_${this.nft.token_id}`;
    }

    async scrapeAndResize() {
        this.logger.debug("img:" + this.imageProperty);
        try {
            this.imageProperty = this.getImageUrl()
            if(this.imageProperty === null || this.imageProperty.length < 5){
                this.logger.debug(`No image found for NFT: ${this.nft.contract}:${this.nft.token_id} from metadata: ${JSON.stringify(this.nft.metadata)}`);
                await this.pool.query(`UPDATE nfts SET scrub_count = 100 WHERE contract = $1 AND token_id = $2`, [this.nft.contract, this.nft.token_id]);
                return;
            }
            this.logger.debug("img:" + this.imageProperty);
            await this.resize();
            await this.updateRowSuccess();
            await this.updateRemoteSuccess();
        } catch (e: Error | any) {
            this.logger.error(`Failure scraping nft: ${this.nft.contract}:${this.nft.token_id} from url: ${this.imageProperty}: ${e.message}`)
            await this.updateRowFailure();
        } 
        try {
            await fs.unlinkSync(this.tmpFile)
        } catch(e: Error | any) {
            this.logger.error(`Failure deleting tmp file: ${this.tmpFile}: ${e}`)
        }
    }

    private getImageUrl(): string | null {
        let imageProperty
        if(typeof this.nft.metadata === "string"){
            this.nft.metadata = JSON.parse(this.nft.metadata);
        }
        if (this.nft.metadata?.image) {
            imageProperty = this.parseProperty(this.nft.metadata.image);
        } else if (this.nft.token_uri) {
            const parts = this.nft.token_uri.split('.');
            const extension = parts[parts.length - 1];
            imageProperty = this.nft.token_uri.trim();
            if(["mp4", "avi", "mpeg"].includes(extension) || imageProperty === null || imageProperty === "___MISSING_TOKEN_URI___"){
                this.logger.error(`No image found for NFT: ${this.nft.contract}:${this.nft.token_id} from metdata: ${JSON.stringify(this.nft.metadata)}`);
                throw new Error(`No image found`)
            }
        } else {    
            return null;
        }

        if (imageProperty.startsWith("ipfs://"))
            imageProperty = imageProperty.replace("ipfs://", `${this.config.ipfsGateway}/`)
        
        if (imageProperty.startsWith("ipfs/"))
            imageProperty = imageProperty.replace("ipfs/", `${this.config.ipfsGateway}/`)
        
        return this.filterGateways(imageProperty);
    }
    private parseProperty(field: any): string {
        let imageProperty;
        if (field && typeof field === "string") {
            imageProperty = field.trim();
        } else if (field && typeof field === "object") {
            if(field.image && typeof field.image === "string"){
                imageProperty = field.image.trim();
            } else if(
                typeof field.image === "object"
                && field.image.description 
                && (field.image.description?.startsWith('ipfs://') || field.image.description?.startsWith('http'))
            ){
                imageProperty = field.image.description.trim()
            } else if(field.description){
                imageProperty = field.description.trim();
            }
        }
        return imageProperty;
    }

    private filterGateways(imageProperty: string): string {
        for (const gatewayUrl of gateways) {
            if (imageProperty?.startsWith(gatewayUrl)) {
                imageProperty = imageProperty.replace(gatewayUrl, `${this.config.ipfsGateway}/`)
            }
        }
        if (imageProperty?.includes('dstor.cloud')) {
            const parts = imageProperty.split('://');
            const subparts = parts[1].split('.');
            imageProperty = parts[0] + "://api";
            for(let i = 1; i < subparts.length;i++){
               imageProperty = imageProperty + '.' + subparts[i]
            }
        }
        for (const gatewayUrl of gatewayDomains) {
            if(imageProperty?.includes(gatewayUrl)){
                const parts = imageProperty.split('://');
                const subparts = parts[1].split('/');
                const subpartsDomain = parts[1].split('.');
                if(subparts[1]?.length > 0 && subpartsDomain[0]?.length > 0){
                    imageProperty = this.config.ipfsGateway + '/' + subpartsDomain[0] + "/" + subparts[1];
                }
            }
        }
        return imageProperty;
    }
    
    private async resize() {
        if(!this.imageProperty || !this.imageProperty.startsWith('http') || this.imageProperty.length > 5000){
            return;   
        }
        await this.downloadFile();  
        await this.resizeFile();
        // TODO: on error, check if dir is empty, delete if it is
    }

    private async downloadFile() {
        if(!this.imageProperty){
            return;   
        }
        const pattern = /^(?:[A-Za-z0-9+\/]{4})*(?:[A-Za-z0-9+\/]{4}|[A-Za-z0-9+\/]{3}=|[A-Za-z0-9+\/]{2}={2})$/;
        const isBase64 = (pattern.test(this.imageProperty) && this.imageProperty.length > 96);
        if(!isBase64){
            try {
                return await pipeline(
                    got.stream(this.imageProperty, {
                        timeout: {
                            lookup: 1000,
                            connect: 5000,
                            secureConnect: 5000,
                            socket: 5000,
                            send: 10000,
                            response: 10000
                        }
                    }),
                    fs.createWriteStream(this.tmpFile)
                )
            } catch (e: Error | any) {
                const errorMsg = `Failure downloading file from ${this.imageProperty}: ${e.message}`;
                this.logger.error(errorMsg)
                throw new Error(errorMsg)
            }
        } else {
            this.logger.error("is base64");
            // Todo: handle base64   
            // Todo: We first need a flag on the collection so we know to update the images regularly
        }
    }
    private async resizeFile() {
        try {
            if (!fs.existsSync(this.targetPath))
                fs.mkdirSync(this.targetPath, {recursive: true});
        
            await sharp(this.tmpFile, {pages: -1}).resize({width: 280})
                .webp()
                .toFile(`${this.targetPath}/280.webp`)

            await sharp(this.tmpFile, {pages: -1}).resize({width: 1440})
                .webp()
                .toFile(`${this.targetPath}/1440.webp`)
        } catch (e: Error | any) {
            const errorMsg = `Failure resizing file from ${this.tmpFile}: ${e.message}`;
            this.logger.error(errorMsg)
            throw new Error(errorMsg)
        }
    }

    private async updateRemoteSuccess() {
        for(const database of this.config.databases){
            const remotePool : Pool = await getPool(database);
            await remotePool.query(`UPDATE nfts SET image_cache = $1 WHERE contract = $2 AND token_id = $3`, [getPath(this.config, this.nft.contract, this.nft.token_id), this.nft.contract, this.nft.token_id]);
        }
    }

    private async updateRowSuccess() {
        await this.pool.query(`UPDATE nfts SET scraped = true WHERE contract = $1 AND token_id = $2`, [this.nft.contract, this.nft.token_id]);
    }

    private async updateRowFailure() {
        const updateSql = `
            UPDATE nfts
            SET scrub_count = scrub_count + 1,
                scrub_last  = now(),
                updated_at = now()
            WHERE contract = $1
            AND token_id = $2
        `;
        const updateValues = [this.nft.contract, this.nft.token_id];
        await this.pool.query(updateSql, updateValues);
    }
}
