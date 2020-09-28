export CGO_CPPFLAGS="-Wno-error -Wno-nullability-completeness -Wno-expansion-to-defined -Wno-builtin-requires-header"
rm -rf mr-*-*
rm -rf mr-out-*
go build -buildmode=plugin ../mrapps/mtiming.go
go run mrworker.go mtiming.so
