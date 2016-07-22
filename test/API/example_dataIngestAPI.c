//
#include "dataIngestAPI.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
//#include <unistd.h> //needed if sleep is used

int main()
{
    dataIngest *obj;

    int chunksize=524288; //1024*512
    int rc;
//    char ch;
    char file_name[25] = "/opt/HiDRA/test_file.cbf";
    FILE *fp;
    char *buffer = malloc(chunksize);
    int bytesRead;

    rc = dataIngest_init (&obj);
    if (rc) exit(-9);

    fp = fopen(file_name,"rb"); // read mode
    assert(fp != NULL);

    rc = dataIngest_createFile (obj, file_name);

    char *printBuf = malloc(100);
    char c;
    int i = 0;
    // read up to sizeof(buffer) bytes
//        while( ( ch = fgetc(fp) ) != EOF )
    while ((bytesRead = fread(buffer, 1, chunksize, fp)) > 0)
    {

        printf ("The content of file %s:\n", file_name);
        printf ("Read file content of size: %i\n", bytesRead);
//        memcpy(printBuf, buffer, 100);
//        printf("%s\n",printBuf);
//        printf("%c",ch);
        rc = dataIngest_write (obj, buffer, bytesRead);
/*
        FILE *fp_local;
        char *filepath = "/opt/HiDRA/data/source/local";
        char abs_filename[128];
        snprintf(abs_filename, sizeof(abs_filename), "%s/%s_%d", filepath, "test.cbf", i);
        printf ("abs_filename %s\n", abs_filename);

        fp_local = fopen(abs_filename,"w");

        fwrite(buffer, chunksize, 1, fp_local);

        fclose(fp_local);
*/
        i++;

    }
    free(printBuf);

    fclose(fp);

//    sleep(5);
    rc = dataIngest_closeFile (obj);

    printf ("Stopping\n");
    rc = dataIngest_stop(obj);

    free (buffer);

    return 0;


};
