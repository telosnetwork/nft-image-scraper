interface Metadata {
    image: string
}

interface NFT {
    contract: string
    token_id: string
    token_uri: string
    metadata: Metadata
}
