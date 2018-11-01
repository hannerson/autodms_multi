import os,sys


def artist_name_process(path,output):
        artist = set()
        f = open(path,"r")
        for line in f:
                arr = line.strip().strip("\n").split("\t")[0].replace(";","###").replace("###","/").split("/")
                for a in arr:
                        artist.add(a.strip())
        f.close()
        fout = open(output,"w+")
        for art in artist:
                fout.write("%s\n" % (art))
        fout.close()


artist_name_process(sys.argv[1],"artist/artist.create." + sys.argv[2])

