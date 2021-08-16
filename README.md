# hotdog-localstack-PoC
PoC for running AWS services(kinesis, dynamodb, lambdas) locally with Localstack

```
alias awslocal="aws --endpoint-url=http://localhost:4566"
```

```
docker network create localstack-tutorial
docker-compose up -d --build
./zip-it.sh
terraform init
terraform apply --auto-approve
awslocal lambda list-functions
awslocal dynamodb list-tables
awslocal kinesis list-streams
```

Note: Make sure you are matching your AWS REGION in docker-compose.yml, terraform provider's region and session.NewSession(). They all need to be the same region.
