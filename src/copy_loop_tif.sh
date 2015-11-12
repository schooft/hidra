#/bin/sh

FILES=/space/projects/test_data/flat/bf_00000.tif
TARGET=/space/projects/live-viewer/data/source/local
i=1
LIMIT=1000
while [ "$i" -le $LIMIT ]
do
    TARGET_FILE="$TARGET/$i.tif"
    echo $TARGET_FILE
    cp $FILES "$TARGET_FILE"
    i=$(($i+1))
done