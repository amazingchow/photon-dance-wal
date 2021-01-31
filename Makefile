test:
	go test -count=1 -v -p 1 $(shell go list ./... | grep -v walpb)

pb-fmt:
	@clang-format -i ./walpb/*.proto