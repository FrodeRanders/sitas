PDF := sysdoc.pdf
TEX := sysdoc.tex

.PHONY: all clean distclean

all: $(PDF)

$(PDF): $(TEX) sitasdoc.cls chapters/*.tex
	pdflatex -interaction=nonstopmode -halt-on-error $(TEX)
	pdflatex -interaction=nonstopmode -halt-on-error $(TEX)

clean:
	rm -f *.aux *.log *.out *.toc *.lof *.lot
	rm -f chapters/*.aux

distclean: clean
	rm -f $(PDF)
