all: build install pull

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