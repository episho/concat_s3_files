# Concatenating files in s3
Function for concatenating multiple files with a given file paths under the specified file path, 
will merge all the files into one file in the given order and delete them.

### Local
This project use golang and [MinIO](https://docs.min.io/docs/minio-quickstart-guide.html).
Make sure that you install [MinIO](https://docs.min.io/docs/minio-quickstart-guide.html) (unless you use docker)

Compile the project using:

```golang
go build
```

and then start the project with:
```
./concat_s3_files concat --bucketName test --password password --region us-east-1 --user user
```

### Docker
To run MinIO using docker, execute the following command:

```
docker run \
-p 9000:9000 \
-p 9001:9001 \
-e "MINIO_ROOT_USER=user" \
-e "MINIO_ROOT_PASSWORD=password" \
quay.io/minio/minio server /data --console-address ":9001"
```