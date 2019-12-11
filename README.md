# postto

A command-line utility for sending data from stdin to an HTTP endpoint, fast.

### Build
```
go build ./cmd/postto/postto.go
``` 

### Run
```
./postto http://$HOST/send/to/path
```

Postto reads its input from stdin. For example:
```
cat file | ./postto http://$HOST/send/to/path
```
or
```
while true; do echo 123; done | ./postto http://$HOST/send/to/path
```
