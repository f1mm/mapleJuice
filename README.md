# CS425-ECE428-MP2



##  How to run the program



Download
------
    git clone https://gitlab.engr.illinois.edu/zchang8/cs425-ece428-mp.git targetDir

cd into mp4
```
cd cs425-ece428-mp/mp4

```

### Start Sever

before starting the MapleJuice program, build functions for go plugin

```

go build -buildmode=plugin -o wc_map.so wc_map.go 
go build -buildmode=plugin -o wc_reduce.so wc_reduce.go 
go build -buildmode=plugin -o rwl_map.so rwl_map.go 
go build -buildmode=plugin -o rwl_reduce.so rwl_reduce.go 

```

start server at each machine so that we can use the distributed log querier to querier logfiles at each machine
```
go run SDFS.go membership.go util.go mapleJuice.go

```

### execute Maple (map)
maple_exe: which application to run, wc (wordcound) or rwl (reverse web link) <br />
num_workers: how many workers to execute the tasks, must be smaller or equal to 10 <br />
prefix: the prefix for sdfs_intermediate file <br />
sdfs_src_folder: src input folder, must already exists on the sdfs <br />

```
4 <maple_exe> <num_workers> <prefix> <sdfs_src_folder>
```

### execute Juice (reduce)
juice_exe: which application to run, wc (wordcound) or rwl (reverse web link) <br />
num_workers: how many workers to execute the tasks, must be smaller or equal to 10 <br />
prefix: the prefix for sdfs_intermediate file, which would be used as the input files for juice tasks <br />
dest_filename: name of the final output file with all the result <br />

```
2 <juice_exe> <num_workers> <prefix> <dest_filename>
```