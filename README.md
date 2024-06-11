# NFT image scraper

This scraper reads NFTs tables on our [Teloscan Indexer](https://github.com/telosnetwork/teloscan-indexer/) databases and will pin media to IPFS before scrapping it, resizing it and saving it.
It will then update the remote database accordingly.
It has a local database to keep track of NFTs' scrap count.

## Operator Setup

### Install Yarn  

```bash
sudo npm install --global yarn
```

### Copy the example config and modify as needed

```bash
cp example.config.json config.json
```
Turn the localIPFS flag on if you are running IPFS locally

### Run the scraper  

```bash
./start.sh
```



