interface Metadata {
    image: any
    properties: any
}

interface NFT {
    erc: number
    block_hash: string
    block_minted: number
    block_created: number
    scraped: boolean
    contract: string
    token_id: string
    token_uri: string
    scrub_count: number
    scrub_last: Date
    metadata: Metadata
    updated_at: Date
}
