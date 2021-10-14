GO=go
GO_TEST=$(GO) test -race -failfast

test:
	@$(GO_TEST) ./...
