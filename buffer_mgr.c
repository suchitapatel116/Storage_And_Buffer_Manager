#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

typedef struct Page
{
	SM_PageHandle data; // represents actual data of the page
	PageNumber pageNum; // Identification integer for each page
	int dirtyBit; // To specify whether the contents of the page has been modified
	int fixCount; // To specify the number of clients using that page at a given instance
	int hitNo;   //  Utilized by LRU algorithm for least recently used page	
	int refNo;   //  Utilized by LFU algorithm for least frequently used page
} FramePage;


// Buffersize represents the size of the buffer pool
int bufferSize = 0;

// r_Index basically stores the count of number of pages read from the disk.
int r_Index = 0;

// noofWrites basically counts the number of I/O write to the disk.
int noofWrites = 0;

// hit is a general count, incremented whenever a page frame is added into the buffer pool.
int hit = 0;

// clock_Pointer is used by CLOCK algorithm to point to the last added page in the buffer pool.
int clock_Pointer = 0;

// lfu_Pointer is used by LFU algorithm to store the least frequently used page.
int lfu_Pointer = 0;


extern void FIFO(BM_BufferPool *const bm, FramePage *page)
{
	//FIFO begins
	FramePage *framePage = (FramePage *) bm->mgmtData;
	
	int i=0, f_Index;
	f_Index = r_Index % bufferSize;


	while (i < bufferSize)
	{
		if(framePage[f_Index].fixCount == 0)
		{
			if(framePage[f_Index].dirtyBit == 1)
			{
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(framePage[f_Index].pageNum, &fh, framePage[f_Index].data);
			// Increasing noofWrites which records the number of writes done by the buffer manager.
			noofWrites++;
			}
			// Setting page frame's content to new page's content
			framePage[f_Index].data = page->data;
			framePage[f_Index].pageNum = page->pageNum;
			framePage[f_Index].dirtyBit = page->dirtyBit;
			framePage[f_Index].fixCount = page->fixCount;
			break;

		}
		else
		{
			// If the current page frame is used by some client, pointer moves to the next location
			f_Index++;
			f_Index = (f_Index % bufferSize == 0) ? 0 : f_Index;
			
			
		}
		i++;
	}
}



extern void LFU(BM_BufferPool *const bm, FramePage *page)
{
	//LFU begins
	FramePage *framePage = (FramePage *) bm->mgmtData;
	
	int x=0;
	int y=0;
	int l_f_Index;
	int l_f_Ref;
	l_f_Index = lfu_Pointer;	
	
	// This method iterates through all the page frames in the buffer pool
	while(x<bufferSize)
	{
		if(framePage[l_f_Index].fixCount == 0)
		{
			l_f_Index = (l_f_Index + x) % bufferSize;
			l_f_Ref = framePage[l_f_Index].refNo;
			break;
		}	
		x++;
	}
	
	x = (l_f_Index + 1) % bufferSize;

	// This method discovers the page frame, which is used the least frequent.
	while(y<bufferSize)
	{
		if(framePage[x].refNo < l_f_Ref)
		{
			l_f_Index = x;
			l_f_Ref = framePage[x].refNo;
		}
		x = (x + 1) % bufferSize;	
		y++;	
	}
	
	// This method verifies if the page in the memory has been modified, then write page to disk	
	if(framePage[l_f_Index].dirtyBit == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(framePage[l_f_Index].pageNum, &fh, framePage[l_f_Index].data);
		
		// Increaments the number of writes done by the buffer manager.
		noofWrites++;
	}
	
	// This method sets page frame's content to new page's content		
	framePage[l_f_Index].data = page->data;
	framePage[l_f_Index].pageNum= page->pageNum;
	framePage[l_f_Index].dirtyBit = page->dirtyBit;
	framePage[l_f_Index].fixCount = page->fixCount;
	lfu_Pointer = l_f_Index + 1;
}


extern void LRU(BM_BufferPool *const bm, FramePage *page)
{	
	FramePage *framePage = (FramePage *) bm->mgmtData;
	int i=0, leastHit_Record, leastHit_Count;

	// Interates through entire page frame in the buffer pool.
	
	while(i<bufferSize)
	{
		if(framePage[i].fixCount==0)
		{
			leastHit_Record=i;
			leastHit_Count=framePage[i].hitNo;
			break;
		}
		i++;
	}
	
	for(i = leastHit_Record + 1; i < bufferSize; i++)
	{
		if(framePage[i].hitNo < leastHit_Count)
		{
			leastHit_Record = i;
			leastHit_Count = framePage[i].hitNo;
		}
	}

	// If page in memory has been remodeled ie..dirtyBit = 1, then write page to disk
	if(framePage[leastHit_Record].dirtyBit == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(framePage[leastHit_Record].pageNum, &fh, framePage[leastHit_Record].data);
		
		// Increaments noofWrites which records the number of writes done by the buffer manager.
		noofWrites++;
	}
	
	// Setting page frame's content to new page's content
	framePage[leastHit_Record].data = page->data;
	framePage[leastHit_Record].pageNum = page->pageNum;
	framePage[leastHit_Record].dirtyBit = page->dirtyBit;
	framePage[leastHit_Record].fixCount = page->fixCount;
	framePage[leastHit_Record].hitNo = page->hitNo;
}


extern void CLOCK(BM_BufferPool *const bm, FramePage *page)
{	
	//CLOCK begins
	FramePage *framePage = (FramePage *) bm->mgmtData;
	while(1)
	{
		clock_Pointer = (clock_Pointer % bufferSize == 0) ? 0 : clock_Pointer;

		if(framePage[clock_Pointer].hitNo == 0)
		{
			// If page in the memory has been altered (dirtyBit = 1), then write page to disk
			if(framePage[clock_Pointer].dirtyBit == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(framePage[clock_Pointer].pageNum, &fh, framePage[clock_Pointer].data);
				
				// Increaments the noofWrites which records the number of writes done by the buffer manager.
				noofWrites++;
			}
			
			// Setting page frame's content to new page's content
			framePage[clock_Pointer].data = page->data;
			framePage[clock_Pointer].pageNum = page->pageNum;
			framePage[clock_Pointer].dirtyBit = page->dirtyBit;
			framePage[clock_Pointer].fixCount = page->fixCount;
			framePage[clock_Pointer].hitNo = page->hitNo;
			clock_Pointer++;
			break;	
		}
		else
		{
			// Incrementing clockPointer so that we can check the next page frame location.
			framePage[clock_Pointer++].hitNo = 0;		
		}
	}
}


/*-----------------------------------------
--------Buffer Functions------------------
-----------------------------------------*/


extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData)
{
	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;

	// Memory space = number of pages x space required for one page
	FramePage *page = malloc(sizeof(FramePage) * numPages);
	bufferSize = numPages;	
	int i=0;

	// Inscribing all the pages in buffer pool. The values of fields (variables) in the page must be either NULL or 0
	while(i < bufferSize)
	{
		page[i].data = NULL;
		page[i].pageNum = -1;
		page[i].dirtyBit = 0;
		page[i].fixCount = 0;
		page[i].hitNo = 0;	
		page[i].refNo = 0;
		i++;
	}
	
	bm->mgmtData = page;
	noofWrites = clock_Pointer = lfu_Pointer = 0;
	return RC_OK;
		
}

// Shutdown buffer pool, thereby removing all the pages from the memory releasing some memory space alongwith freeing up all resources 

extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
	FramePage *framePage = (FramePage *)bm->mgmtData;
	// Write all Altered pages back to disk
	forceFlushPool(bm);

	int i=0;	
	while(i < bufferSize)
	{
		// If fixCount != 0, that is the contents of the pages were altered by some client and has not been written back to disk.
		if(framePage[i].fixCount != 0)
		{
			return RC_PINNED_PAGES_IN_BUFFER;
		}
		i++;
	}

	// unleasing space occupied by the page
	free(framePage);
	bm->mgmtData = NULL;
	return RC_OK;
}


// This function writes all the dirty pages (having fixCount = 0) to disk
extern RC forceFlushPool(BM_BufferPool *const bm)
{
	FramePage *framePage = (FramePage *)bm->mgmtData;
	
	int i=0;
	// Accumulates all the modified pages in memory to page file on disk
	while(i<bufferSize)
	{
		if(framePage[i].fixCount == 0 && framePage[i].dirtyBit == 1)
		{
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(framePage[i].pageNum, &fh, framePage[i].data);
			framePage[i].dirtyBit = 0;
			noofWrites++;
		}
		i++;
	}
	
	return RC_OK;
}


/*------------------------------------------------
--------Page Management functions-----------------
-------------------------------------------------*/


extern RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	FramePage *framePage = (FramePage *)bm->mgmtData;
	
	int i=0;
	// This method iterates all the pages in buffer pool
	while(i<bufferSize)
	{
		if(framePage[i].pageNum == page->pageNum)
		{
			framePage[i].dirtyBit = 1;
			return RC_OK;
		}	
		i++;
	}	
	return RC_ERROR;
}

// This function removes the page from the memory
extern RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{	
	FramePage *framePage = (FramePage *)bm->mgmtData;
	
	int i=0;
	while(i<bufferSize)
	{
		if(framePage[i].pageNum == page->pageNum)
		{
			framePage[i].fixCount--;
			break;
		}
		i++;
	}
	// Iterating through all the pages in the buffer pool
		
	return RC_OK;
}


extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	FramePage *framePage = (FramePage *)bm->mgmtData;
	
	int i=0;
	while(i<bufferSize)
	{
		if(framePage[i].pageNum == page->pageNum)
		{
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(framePage[i].pageNum, &fh, framePage[i].data);
			framePage[i].dirtyBit = 0;
			noofWrites++;

		}
		i++;
	}
		return RC_OK;
}


extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum)
{
	FramePage *framePage = (FramePage *)bm->mgmtData;
	
	// Examining if buffer pool is empty or not

	if(framePage[0].pageNum == -1)
	{
		// Reading page from disk and inscribing page frame's content in the buffer pool
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		framePage[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
		ensureCapacity(pageNum,&fh);
		readBlock(pageNum, &fh, framePage[0].data);
		framePage[0].pageNum = pageNum;
		framePage[0].fixCount++;
		r_Index = hit = 0;
		framePage[0].hitNo = hit;	
		framePage[0].refNo = 0;
		page->pageNum = pageNum;
		page->data = framePage[0].data;
		
		return RC_OK;		
	}
	else
	{	
		int i;
		bool isBufferFull = true;
		
		for(i = 0; i < bufferSize; i++)
		{
			if(framePage[i].pageNum != -1)
			{	
				// Examining if page is in memory
				if(framePage[i].pageNum == pageNum)
				{
					// Increamenting fixCount i.e. now there is one more client accessing this specific page
					framePage[i].fixCount++;
					isBufferFull = false;
					hit++; 
					if(bm->strategy == RS_LRU)
						// LRU algorithm uses the value of hit to determine the least recently used page	
						framePage[i].hitNo = hit;
					else if(bm->strategy == RS_CLOCK)
						// hitNo = 1 in order to represent that this was the last page frame checked
						framePage[i].hitNo = 1;
					else if(bm->strategy == RS_LFU)
						// Increasing the refNo in order to add one more count of number of times the page is used
						framePage[i].refNo++;
					
					page->pageNum = pageNum;
					page->data = framePage[i].data;

					clock_Pointer++;
					break;
				}				
			} else {
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				framePage[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
				readBlock(pageNum, &fh, framePage[i].data);
				framePage[i].pageNum = pageNum;
				framePage[i].fixCount = 1;
				framePage[i].refNo = 0;
				r_Index++;	
				hit++; 
				if(bm->strategy == RS_LRU)
					// LRU algorithm uses the value of hit to determine the least recently used page
					framePage[i].hitNo = hit;				
				else if(bm->strategy == RS_CLOCK)
					// hitNo = 1 to indicate that this was the last page frame examined (added to the buffer pool)
					framePage[i].hitNo = 1;
						
				page->pageNum = pageNum;
				page->data = framePage[i].data;
				
				isBufferFull = false;
				break;
			}
		}
		
	// If isBufferFull = true, then it means that the buffer is full and we must replace an existing page using page replacement strategy
		if(isBufferFull == true)
		{
			// Create a new page to store data read from the file.
			FramePage *newPage = (FramePage *) malloc(sizeof(FramePage));		
			
			// Reading page from disk and initializing page frame's content in the buffer pool
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			newPage->data = (SM_PageHandle) malloc(PAGE_SIZE);
			readBlock(pageNum, &fh, newPage->data);
			newPage->pageNum = pageNum;
			newPage->dirtyBit = 0;		
			newPage->fixCount = 1;
			newPage->refNo = 0;
			r_Index++;
			hit++;

			if(bm->strategy == RS_LRU)
				// LRU algorithm uses the value of hit to determine the least recently used page
				newPage->hitNo = hit;				
			else if(bm->strategy == RS_CLOCK)
				// hitNo = 1 to represent that this was the last page frame examined (added to the buffer pool)
				newPage->hitNo = 1;

			page->pageNum = pageNum;
			page->data = newPage->data;			

		// Calls respective algorithm's function depending on the page replacement strategy selected (passed through parameters)
			switch(bm->strategy)
			{			
				case RS_FIFO: // Using FIFO algorithm
					FIFO(bm, newPage);
					break;
				
				case RS_LRU: // Using LRU algorithm
					LRU(bm, newPage);
					break;
				
				case RS_CLOCK: // Using CLOCK algorithm
					CLOCK(bm, newPage);
					break;
  				
				case RS_LFU: // Using LFU algorithm
					LFU(bm, newPage);
					break;
  				
				case RS_LRU_K:
					printf("\n LRU-k algorithm not implemented");
					break;
				
				default:
					printf("\nAlgorithm Not Implemented\n");
					break;
			}
						
		}		
		return RC_OK;
	}	
}



/*--------------------------------------
---------Statistical Functions----------
--------------------------------------*/


// This function returns an array of page numbers
extern PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	PageNumber *frameContents = malloc(sizeof(PageNumber) * bufferSize);
	FramePage *framePage = (FramePage *) bm->mgmtData;
	
	int x = 0;
	
	for(x=0;x<bufferSize;x++)
	{
		frameContents[x] = (framePage[x].pageNum != -1) ? framePage[x].pageNum : NO_PAGE;
	}
	
	return frameContents;
}

extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
	bool *dirtyFlags = malloc(sizeof(bool) * bufferSize);
	FramePage *framePage = (FramePage *)bm->mgmtData;
	// Iterating through all the pages in the buffer pool and setting dirtyFlags' value to TRUE if page is dirty else FALSE
	int i=0;
	while(i<bufferSize)
	{
		if(framePage[i].dirtyBit==1)
		{
			dirtyFlags[i] = true;
		}
		else
		{
			dirtyFlags[i] = false;
		}
		i++;
	}
	
	return dirtyFlags;
}


extern int *getFixCounts (BM_BufferPool *const bm)
{
	int *fixCounts = malloc(sizeof(int) * bufferSize);
	FramePage *framePage= (FramePage *)bm->mgmtData;
	
	int i;
	for(i=0;i<bufferSize;i++)
	{
		fixCounts[i] = (framePage[i].fixCount != -1) ? framePage[i].fixCount : 0;
	}
	return fixCounts;
	
	// Iterating through all the pages in the buffer pool and setting fixCounts' value to page's fixCount
	
}

// This function returns the number of pages that have been read from disk since a buffer pool has been initialized.
extern int getNumReadIO (BM_BufferPool *const bm)
{
		return (r_Index + 1);
}

// This function returns the number of pages written to the page file since the buffer pool has been initialized.
extern int getNumWriteIO (BM_BufferPool *const bm)
{
	return noofWrites;
}



