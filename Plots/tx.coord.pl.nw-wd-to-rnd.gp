#set terminal png size 800,600
#set output 'imgs/tx.coord.pl.nw-wd-to-rnd.png'

set multiplot

set size 1,0.33
set origin 0.0,0.66

set key left top
set xlabel "Number of clients"
set ylabel "Throuthput (op/s)"

plot "logs/tx.coord.occ-pl.no-wait" using 1:2 lt rgb "black" title "no wait" with lines,\
     "logs/tx.coord.occ-pl.wait-die" using 1:2 lt rgb "red" title "wait die" with lines,\
     "logs/tx.coord.occ-pl.to" using 1:2 lt rgb "blue" title "to" with lines,\
     "logs/tx.coord.occ-pl.random" using 1:2 lt rgb "green" title "random" with lines,\

set size 0.5,0.66
set origin 0.0,0.0

set key left top
set xlabel "Number of clients"
set ylabel "Max read latency (us)"

plot "logs/tx.coord.occ-pl.no-wait" using 1:4 lt rgb "black" title "no wait" with lines,\
     "logs/tx.coord.occ-pl.wait-die" using 1:4 lt rgb "red" title "wait die" with lines,\
     "logs/tx.coord.occ-pl.to" using 1:4 lt rgb "blue" title "to" with lines,\
     "logs/tx.coord.occ-pl.random" using 1:4 lt rgb "green" title "random" with lines,\

set size 0.5,0.66
set origin 0.5,0.0

set key left top
set xlabel "Number of clients"
set ylabel "max write latency (us)"

plot "logs/tx.coord.occ-pl.no-wait" using 1:9 lt rgb "black" title "no wait" with lines,\
     "logs/tx.coord.occ-pl.wait-die" using 1:9 lt rgb "red" title "wait die" with lines,\
     "logs/tx.coord.occ-pl.to" using 1:9 lt rgb "blue" title "to" with lines,\
     "logs/tx.coord.occ-pl.random" using 1:9 lt rgb "green" title "random" with lines,\

unset multiplot