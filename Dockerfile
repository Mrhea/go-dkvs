FROM golang:latest

# install dep
RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

# change working directory, copy files & get dependencies
WORKDIR $GOPATH/src/github.com/mrhea/CMPS128_Assignment4
COPY . .
RUN dep ensure --vendor-only
RUN go build main.go

CMD [ "./main" ]
