export interface ScraperConfig {
    databases: array,
    ipfsGateway: string
    querySize: number
    queueConcurrency: number
    rootDir: string
    tempDir: string
    rootUrl: string
    logLevel: string
}
