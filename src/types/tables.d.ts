interface Metadata {
    image: any
    properties: any
}

interface NFT {
    contract: string
    token_id: string
    token_uri: string
    metadata: Metadata
}
