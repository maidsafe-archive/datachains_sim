# Script for plotting the data about the network structure
# Usage: gnuplot -c plot.gp data-file-name output-name.png

set terminal png size 1920,1080
set output ARG2
set ytics nomirror
set y2tics
plot ARG1 title 'Network size' with lines lc rgb "#FF0000", \
     ARG1 u 1:3 title 'Number of sections' axes x1y2 with lines lc rgb "#0000FF"
