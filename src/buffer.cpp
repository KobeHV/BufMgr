/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

    BufMgr::BufMgr(std::uint32_t bufs)
            : numBufs(bufs) {
        bufDescTable = new BufDesc[bufs];

        for (FrameId i = 0; i < bufs; i++) {
            bufDescTable[i].frameNo = i;
            bufDescTable[i].valid = false;
        }

        bufPool = new Page[bufs];

        int htsize = ((((int) (bufs * 1.2)) * 2) / 2) + 1;
        hashTable = new BufHashTbl(htsize);  // allocate the buffer hash table

        clockHand = bufs - 1;
    }

/** 
 * @author kobe 
 * @time 2019/5/9
 */
    BufMgr::~BufMgr() {
        for (FrameId i = 0; i < numBufs; i++) {
            if (bufDescTable[i].dirty == true) {
                flushFile(bufDescTable[i].file);
            }
        }
        delete hashTable;
        delete[] bufDescTable;
        delete[] bufPool;        
    }

    void BufMgr::advanceClock() {
        clockHand = (clockHand + 1) % numBufs;
    }

    void BufMgr::allocBuf(FrameId &frame) {
        while (true) {
            for (FrameId i = 0; i != numBufs; i++) {
                advanceClock();

                BufDesc *tmpBufDesc = &bufDescTable[clockHand];
                if (!tmpBufDesc->valid) {
                    try {
                        if (tmpBufDesc->file) {
                            hashTable->remove(tmpBufDesc->file,bufDescTable->pageNo);
                        }                        
                    } catch (HashNotFoundException &e) {
                    }
                    frame = clockHand;
                    bufStats.diskreads += 1;
                    return;
                }
                if (tmpBufDesc->pinCnt > 0) {
                    continue;
                }
                if (tmpBufDesc->refbit) {
                    tmpBufDesc->refbit = false;
                    continue;
                }
                if (tmpBufDesc->dirty) {
                    flushFile(tmpBufDesc->file);
                    tmpBufDesc->dirty = false;
                }            
                try {
                    if (tmpBufDesc->file) {
                        hashTable->remove(tmpBufDesc->file, tmpBufDesc->pageNo);
                        tmpBufDesc->Clear();
                    }                  
                }catch (HashNotFoundException &e) {
                }
                frame = clockHand;//return
                bufStats.diskreads++;
                return;                
            }
            
            bool flag = false;
            for (FrameId i = 0; i < numBufs; i++) {
                if (!bufDescTable[i].pinCnt) {
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                throw BufferExceededException();
            }
        }
    }


    void BufMgr::readPage(File *file, const PageId pageNo, Page *&page) {
        FrameId frameNo;
        try {
            hashTable->lookup(file, pageNo, frameNo);
            bufDescTable[frameNo].refbit = true;
            bufDescTable[frameNo].pinCnt++;
            
        } catch (HashNotFoundException &e) {
            allocBuf(frameNo);
            Page rp = file->readPage(pageNo);
            bufDescTable[frameNo].Set(file, pageNo);
            hashTable->insert(bufDescTable[frameNo].file,
                              bufDescTable[frameNo].pageNo, frameNo);
        
            bufPool[frameNo] = rp;
            
        }
        page = &bufPool[frameNo];//need out the "try-catch" ?
        bufStats.accesses++;
    }


    void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty) {
        FrameId frameNo;
        try {
            hashTable->lookup(file, pageNo, frameNo);
            if (bufDescTable[frameNo].pinCnt > 0) {
                bufDescTable[frameNo].pinCnt--;
            } else if (bufDescTable[frameNo].pinCnt == 0) {
                throw PageNotPinnedException(file->filename(), pageNo, frameNo);
            }
            if (dirty) {
                bufDescTable[frameNo].dirty = true;
            }
        } catch (HashNotFoundException &e) {
            //cerr << e << endl;
            return;
        }
    }

    void BufMgr::flushFile(const File *file) {
        FrameId fID;
        PageId pID;

        for (FrameId i = 0; i < numBufs; i++) {
            BufDesc *tmpBufDesc = &bufDescTable[i];
            if (tmpBufDesc->file == file) {
                fID = tmpBufDesc->frameNo;
                pID = tmpBufDesc->pageNo;
                if (tmpBufDesc->pinCnt > 0) {
                    throw PagePinnedException(file->filename(), pID, fID);
                } else if (tmpBufDesc->valid == false) {
                    throw BadBufferException(fID, tmpBufDesc->dirty, tmpBufDesc->valid,
                                             tmpBufDesc->refbit);
                } else if (tmpBufDesc->pinCnt == 0 && tmpBufDesc->valid == true
                           && tmpBufDesc->dirty) {
                    tmpBufDesc->file->writePage(bufPool[fID]);
                    tmpBufDesc->dirty = false;
                    bufStats.diskwrites++;
                }
                hashTable->remove(file, pID);
                tmpBufDesc->Clear();

            }
        }

    }

    void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page) {
        FrameId frameNo;
        allocBuf(frameNo);
        bufPool[frameNo] = file->allocatePage();
        page = &bufPool[frameNo];
        pageNo = page->page_number();

        bufDescTable[frameNo].Set(file, pageNo);
        hashTable->insert(file, pageNo, frameNo);        
    }

    void BufMgr::disposePage(File *file, const PageId PageNo) {
        FrameId frameNo;
        try {
            hashTable->lookup(file, PageNo, frameNo);
            hashTable->remove(file, PageNo);
            bufDescTable[frameNo].Clear();
        } catch (HashNotFoundException &e) {
        }
        file->deletePage(PageNo);
    }

    void BufMgr::printSelf(void) {
        BufDesc *tmpbuf;
        int validFrames = 0;

        for (std::uint32_t i = 0; i < numBufs; i++) {
            tmpbuf = &(bufDescTable[i]);
            std::cout << "FrameNo:" << i << " ";
            tmpbuf->Print();

            if (tmpbuf->valid == true)
                validFrames++;
        }

        std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }

}
