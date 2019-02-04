all: build install pull

stack:
	@docker build --tag=127.0.0.1:5000/lfmc-backend .
	@docker push 127.0.0.1:5000/lfmc-backend
	
build:
	@docker build --tag=anthonyrawlinsuom/lfmc-backend .
	
install:
	@docker push anthonyrawlinsuom/lfmc-backend
	
pull:
	@docker pull anthonyrawlinsuom/lfmc-backend
	
release:
	./release.sh

clean:
	@docker rmi --force anthonyrawlinsuom/lfmc-backend