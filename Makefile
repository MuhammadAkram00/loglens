.PHONY: test demo
test:
	python3 -m unittest -v

demo:
	python3 generate_sample.py --count 12000 --out sample.jsonl
	python3 loglens.py ingest sample.jsonl
	python3 loglens.py summary
	python3 loglens.py report --out report.html
