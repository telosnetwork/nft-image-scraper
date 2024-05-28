export interface ScraperConfig {
    databases: DatabaseConfig[],
    database: DatabaseConfig,
    ipfsGateway: string
    querySize: number
    queueConcurrency: number
    rootDir: string
    tempDir: string
    rootUrl: string
    logLevel: string
    localIpfs: boolean
}

export interface DatabaseConfig {
    name: string;
    user: string;
    password: string;
    host: string;
    port: number;
}