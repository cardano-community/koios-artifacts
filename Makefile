SQLFLUFF=sqlfluff
DIALECT=postgres
SQL_WORKDIR=files/grest

.PHONY: lint fix format check clean

lint:
	cd $(SQL_WORKDIR) && $(SQLFLUFF) lint --dialect $(DIALECT) .

fix:
	cd $(SQL_WORKDIR) && $(SQLFLUFF) fix --dialect $(DIALECT) .

format: fix

check:
	cd $(SQL_WORKDIR) && $(SQLFLUFF) lint --dialect $(DIALECT) .

clean:
	rm -rf $(SQL_WORKDIR)/.sqlfluff_cache
