mkdir -p ../html
cp ../*.txt ../*.itxt ./
# sphinx specific index
cp index.rst index.txt
sphinx-build -E -b html -d ../html/doctrees . ../html
cd ../html && pwd
