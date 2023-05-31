import {Pool} from "pg";
import sharp from 'sharp';
import got from 'got';
import fs from "fs";
import {ScraperConfig} from "./types/configs.js";
import {createLogger} from "./util/logger.js";
import stream from 'node:stream';
import {promisify} from 'node:util'

const pipeline = promisify(stream.pipeline);
const logger = createLogger('Scraper');

const gateways = [
    "https://gateway.pinata.cloud/ipfs/", "https://nftstorage.link/ipfs/", "https://kitchen.mypinata.cloud/ipfs/"
]
const gatewayDomains = [
    "nftstorage.link"
]

export default class Scraper {
    private readonly targetPath: string;
    private cacheUrl: string;
    private imageProperty?: string;
    private tmpFile: string;

    constructor(private pool: Pool, private nft: NFT, private config: ScraperConfig) {
        this.targetPath = `${this.config.rootDir}/${this.nft.contract}/${this.nft.token_id}`
        this.cacheUrl = `${config.rootUrl}/${this.nft.contract}/${this.nft.token_id}`
        this.tmpFile = `${config.tempDir}/${this.nft.contract}_${this.nft.token_id}`;
    }

    async scrapeAndResize() {
        try {
            this.imageProperty = this.getImageUrl()
            await this.resize();
            await this.updateRowSuccess();
        } catch (e: Error | any) {
            logger.error(`Failure scraping nft: ${this.nft.contract}:${this.nft.token_id} from url: ${this.imageProperty}: ${e.message}`)
            await this.updateRowFailure();
        } finally {
            await fs.unlinkSync(this.tmpFile)
        }
    }

    private parseProperty(field: any): string {
        let imageProperty
        if (field && typeof field === "string") {
            imageProperty = field.trim()
        } else if (field && typeof field === "object") {
            if(field.image && typeof field === "string"){
                imageProperty = this.nft.metadata.image.image.trim()
            } else if(
                typeof field.image === "object"
                && field.image.description 
                && (field.image.description?.startsWith('ipfs://') || this.nft.metadata.image.description?.startsWith('http'))
            ){
                imageProperty = this.nft.metadata.image.description.trim()
            } 
        }
        return imageProperty;
    }
    private getImageUrl(): string {
        let imageProperty;
        if (this.nft.metadata.image) {
            imageProperty = this.parseProperty(this.nft.metadata.image);
        } else if(this.nft.metadata.properties){
            imageProperty = this.parseProperty(this.nft.metadata.properties);
        } else if(!this.nft.metadata) {
            const parts = this.nft.token_uri.split('.');
            const extension = parts[parts.length - 1];
            imageProperty = this.nft.token_uri.trim();
            if(["mp4", "avi", "mpeg"].includes(extension) || imageProperty === null || imageProperty === "___MISSING_TOKEN_URI___"){
                logger.error(`No image found for NFT: ${this.nft.contract}:${this.nft.token_id} from metdata: ${JSON.stringify(this.nft.metadata)}`);
                throw new Error(`No image found!!`)
            }
        }
        
        if(!imageProperty)
            return '';

        if (imageProperty?.startsWith("ipfs://"))
            imageProperty = imageProperty.replace("ipfs://", `${this.config.ipfsGateway}/`)
        
        if (imageProperty?.startsWith("ipfs/"))
            imageProperty = imageProperty.replace("ipfs/", `${this.config.ipfsGateway}/`)
        
        return this.filterGateways(imageProperty);
    }

    private filterGateways(imageProperty: string){
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
        await this.downloadFile();
        await this.resizeFile();
        // TODO: on error, check if dir is empty, delete if it is
    }

    private async downloadFile() {
        if(!this.imageProperty){
            return;   
        }
        const pattern = /^(?:[A-Za-z0-9+\/]{4})*(?:[A-Za-z0-9+\/]{4}|[A-Za-z0-9+\/]{3}=|[A-Za-z0-9+\/]{2}={2})$/;
        const isBase64 = pattern.test(this.imageProperty);
        if(!isBase64){
            try {
                await pipeline(
                    got.stream(this.imageProperty, {
                        timeout: {
                            lookup: 3000,
                            connect: 8000,
                            secureConnect: 8000,
                            socket: 1500,
                            send: 10000,
                            response: 12000
                        }
                    }),
                    fs.createWriteStream(this.tmpFile)
                )
            } catch (e) {
                const errorMsg = `Failure downloading file from ${this.imageProperty}`;
                logger.error(errorMsg)
                throw new Error(errorMsg)
            }
        } else {
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
        } catch (e) {
            const errorMsg = `Failure resizing file from ${this.tmpFile}`;
            logger.error(errorMsg)
            throw new Error(errorMsg)
        }
    }

    private async updateRowSuccess() {
        const updateSql = `UPDATE nfts
                           SET image_cache = $1
                           WHERE contract = $2
                             AND token_id = $3`;
        const updateValues = [this.cacheUrl, this.nft.contract, this.nft.token_id];
        await this.pool.query(updateSql, updateValues);
    }

    private async updateRowFailure() {
        const updateSql = `UPDATE nfts
                           SET scrub_count = scrub_count + 1,
                               scrub_last  = now()
                           WHERE contract = $1
                             AND token_id = $2`;
        const updateValues = [this.nft.contract, this.nft.token_id];
        await this.pool.query(updateSql, updateValues);
    }
}
