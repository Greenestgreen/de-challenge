# Deployment

 

The application/ETL `de-challenge`  can be deployed through Docker. This docker image contains a Spark cluster ready to submit jobs to it. This image also shares storage with the host from where it takes the input data and writes the output. To run the application just execute:
```
docker-compose up
```

### Variables

The docker file contains this system variables that the application uses:

- INPUT_RESULT:  Location where the `result.csv` file is

- INPUT_CONSOLES: Location where the `consoles.csv` file is

- OUTPUT_FOLDER: Location where files will be written

### Volumnes
The folder `data` of this project is shared within the docker container of this image. Where also it will write the output by default


### Architecture
![Architecture](https://github.com/Greenestgreen/de-challenge/blob/feature/games-etl/Deployment/Architecture.png?raw=true)


