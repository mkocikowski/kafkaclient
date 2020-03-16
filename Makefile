.PHONY: unit
unit:
	go test ./... -run=Unit

# stand up kafka: cd test ; docker-compose build ; docker-compose up
.PHONY: integration
integration:
	go test ./... -count=1
