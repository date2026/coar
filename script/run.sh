#!/bin/bash
DIR=/home/openec/coar

cd $DIR/build


for((i=1;i<=50;i++));
do 
{
    echo "repair in node02"
    /home/openec/coar/build/ECClient decode /input_1024MB /home/openec/coar/conf/ecdag_decode_2 0 2 3 4087 1
    echo "repair in node03"
    /home/openec/coar/build/ECClient decode /input_1024MB /home/openec/coar/conf/ecdag_decode_3 0 1 3 4087 2
    echo "repair in node04"
    /home/openec/coar/build/ECClient decode /input_1024MB /home/openec/coar/conf/ecdag_decode 0 1 2 4087 3
}
done