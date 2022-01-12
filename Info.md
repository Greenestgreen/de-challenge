# ETL de-challenge

## Basics
The core of this program runs on `pypsark` and can be deployed using the `docker files` in this repostiry.

In order to do so just run: 
```
docker-compose up
```
Make sure that docker  sharing volumes in your system is available 
## Input
This program consumes the csv files inside the `./data` folder.

## Output
The result/output of this program will appear on `./data/exoport` folder.

## Testing
This programs uses `pytest` for testing.

## Extras
In order to change the `input` or `output` folders, the `ENV` variables on the `Dockerfile` file should be changed. More info about this in the Deployment folder