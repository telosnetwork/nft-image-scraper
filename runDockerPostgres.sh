INSTALL_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run -itd --rm \
    --name nft-scraper-postgres \
    -e POSTGRES_PASSWORD=password \
    -e PGDATA=/var/lib/postgresql/data/pgdata \
    -v $INSTALL_ROOT/data:/var/lib/postgresql/data \
    -p 5134:5432 \
    postgres