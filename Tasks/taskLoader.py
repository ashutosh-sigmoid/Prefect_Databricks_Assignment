from setLogger import set_logger

log=set_logger()
dir_name='/dbfs/FileStore/tables/Ashutosh/'
def loadIntoTxt(data,file_name):
    log.info("Load into text file")
    with open(dir_name+file_name, "w") as f:
        f.write(data)
        f.close()
    log.info(f"{file_name} loaded ")    
        
def readIntoTxt(file_name):
    log.info("read text file")
    with open(dir_name+file_name,"r") as f:
        log.info(f"{f.read()}")
        f.close()
        
        
       
        
        