.PHONY: test-once
test-once:
	go test -timeout 1000s -race

.PHONY: test-all
test-all: test-3a test-3b test-3c test-3d

.PHONY: test-3a
test-3a:
	go test -timeout 30s -run 3A -race

.PHONY: test-3b
test-3b:
	go test -timeout 600s -count=10 -run 3B -race

.PHONY: test-3c
test-3c:
	go test -timeout 1000s -count=5 -run 3C -race

.PHONY: test-3d
test-3d:
	go test -timeout 1000s -count=5 -run 3D -race
