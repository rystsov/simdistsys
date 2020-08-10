#set terminal png size 800,600
#set output 'imgs/tx.coord.wd.2pc-vs-pl.png'

set multiplot

set size 1,0.33
set origin 0.0,0.66

set key left top
set xlabel "Number of clients"
set ylabel "Throuthput (op/s)"

plot "logs/tx.coord.occ-2pc.wait-die" using 1:2 lt rgb "black" title "wd occ-2pc" with lines,\
     "logs/tx.coord.occ-pl.wait-die" using 1:2 lt rgb "red" title "wd occ-pl" with lines,\

set size 0.5,0.66
set origin 0.0,0.0

set key left top
set xlabel "Number of clients"
set ylabel "Max read latency (us)"

plot "logs/tx.coord.occ-2pc.wait-die" using 1:4 lt rgb "black" title "wd occ-2pc" with lines,\
     "logs/tx.coord.occ-pl.wait-die" using 1:4 lt rgb "red" title "wd occ-pl" with lines,\

set size 0.5,0.66
set origin 0.5,0.0

set key left top
set xlabel "Number of clients"
set ylabel "max write latency (us)"

plot "logs/tx.coord.occ-2pc.wait-die" using 1:9 lt rgb "black" title "wd occ-2pc" with lines,\
     "logs/tx.coord.occ-pl.wait-die" using 1:9 lt rgb "red" title "wd occ-pl" with lines,\

unset multiplot