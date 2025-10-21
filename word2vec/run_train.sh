# make


# if [ ! -e text8 ]; then
#   wget http://mattmahoney.net/dc/text8.zip -O text8.gz
#   gzip -d text8.gz -f
# fi


time ./word2vec -train text8 -output vectors.cbow.txt -cbow 1 -size 100 -window 8 -negative 25 -hs 0 -sample 1e-4 -threads 10 -binary 0 -iter 30

time ./word2vec -train text8 -output vectors.cbow.binary -cbow 1 -size 100 -window 8 -negative 25 -hs 0 -sample 1e-4 -threads 10 -binary 1 -iter 30

time ./word2vec -train text8 -output vectors.skipgram.txt -cbow 0 -size 100 -window 8 -negative 25 -hs 0 -sample 1e-4 -threads 10 -binary 0 -iter 30

time ./word2vec -train text8 -output vectors.skipgram.binary -cbow 0 -size 100 -window 8 -negative 25 -hs 0 -sample 1e-4 -threads 10 -binary 1 -iter 30




# ./distance vectors.skipgram.binary
