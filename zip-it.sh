cd ./lambda/dogCatcher
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o main main.go
zip -r ../../dogCatcher.zip main
rm main
cd ../..

cd ./lambda/dogProcessor
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o main main.go
zip -r ../../dogProcessor.zip main
rm main
cd ../..

cd ./lambda/hotDogDespatcher
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o main main.go
zip -r ../../hotDogDespatcher.zip main
rm main
cd ../..