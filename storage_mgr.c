#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>

#include "storage_mgr.h"

FILE *pageFile;

extern void initStorageManager (void) {
	
	pageFile = NULL;
}

//Creates an empty page in memory
extern RC createPageFile (char *fileName) {
	//'w+' mode in fopen creates an empty file 'fileName' for read and write purpose
	pageFile = fopen(fileName, "w+");

	//Checking if the file, stored in 'filePointer' is successufully opened or not, if yes then  
	if(pageFile == NULL) {
		return RC_FILE_NOT_FOUND;
	} else {
		//Creating blank page in memory, to store no. of 'PAGE_SIZE' elements, each of size of character
		SM_PageHandle emptyPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
		
		//fwrite(buffer, item size, max no of lines, file handle where to write the data) returns the size of data written
		if(fwrite(emptyPage, sizeof(char), PAGE_SIZE,pageFile) < PAGE_SIZE)
			printf("write failed \n");
		else
			printf("write succeeded \n");
		
		//Closing output file stream so that all buffers are flushed
		fclose(pageFile);
		
		//memory deallocation to ensure proper memory management
		free(emptyPage);
		
		return RC_OK;
	}
}

//Open the file in read mode and set the values in file structure
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
	//Open the file in read mode
	pageFile = fopen(fileName, "r");

	// Checking if file was successfully opened.
	if(pageFile == NULL) {
		return RC_FILE_NOT_FOUND;
	} else { 
		// Updating file handle's filename and set the current position to the start of the page.
		fHandle->fileName = fileName;
		fHandle->curPagePos = 0;

		//Check the no of pages
		struct stat fileInfo;
		if(fstat(fileno(pageFile), &fileInfo) < 0)    
			return RC_ERROR;
		fHandle->totalNumPages = fileInfo.st_size/ PAGE_SIZE;

		 
		fclose(pageFile);
		return RC_OK;
	}
}

//If the file is opened for reading or writing then it will be closed 
extern RC closePageFile (SM_FileHandle *fHandle) {
	
	if(pageFile != NULL)
		pageFile = NULL;	
	return RC_OK; 
}

//Delete the page file
extern RC destroyPageFile (char *fileName) {
	// Opening file stream in read mode. 'r' mode creates an empty file for reading purpose only.	
	pageFile = fopen(fileName, "r");
	
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND; 
	
	remove(fileName);
	return RC_OK;
}

//Read from file to block in memory
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	
	//Condition to check pagenumber value
	if (pageNum > fHandle->totalNumPages || pageNum < 0)
        	return RC_READ_NON_EXISTING_PAGE;

	//opening file in read mode - opens empty file
	pageFile = fopen(fHandle->fileName, "r");

	//condition to check fileopen statement
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND;
	
	//Adjust the position of the pointer filestream, position is evaluated by pagenumber * PAGE_SIZE
	//seek is complete when it is equal to 0
	int isSeekSuccess = fseek(pageFile, (pageNum * PAGE_SIZE), SEEK_SET);
	if(isSeekSuccess == 0) {
		//Reading the content, storing it to the location set out by memPage
		if(fread(memPage, sizeof(char), PAGE_SIZE, pageFile) < PAGE_SIZE)
			return RC_ERROR;
	} else {
		return RC_READ_NON_EXISTING_PAGE; 
	}
    	
	//Setting the current page position to the pointer
	//After reading 1 block/page what is the current posotion of the pointer in file. It returns the current location in file stream
	fHandle->curPagePos = ftell(pageFile); 
	
	// Closing file stream so that all the buffers are flushed.     	
	fclose(pageFile);
	
    	return RC_OK;
}

extern int getBlockPos (SM_FileHandle *fHandle) {
	//Returning current position of page, retrieved from the fhandle. It will be same as file position
	return fHandle->curPagePos;
}

extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//'r' mode opens the file for read purpose	
	pageFile = fopen(fHandle->fileName, "r");
	
	//It checks if file was successfully opened or not
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND;

	int i;
	for(i = 0; i < PAGE_SIZE; i++) {
		//Single character from file is read
		char c = fgetc(pageFile);
	
		//Checks if the end of file is reached. 
		if(feof(pageFile))
			break;
		else
			memPage[i] = c;
	}
	
	fHandle->curPagePos = ftell(pageFile); 

	//All buffers are flushed after closing the file.
	fclose(pageFile);
	return RC_OK;
}

extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {	
	//If we are on the first block and no other previous block
	if(fHandle->curPagePos <= PAGE_SIZE) {
		printf("\n First block: Previous block not present.");
		return RC_READ_NON_EXISTING_PAGE;	
	} else {
		//Calculates current page number by dividing page size by current page position	
		int currentPageNumber = fHandle->curPagePos / PAGE_SIZE;
		int startPosition = (PAGE_SIZE * (currentPageNumber - 2));

		//'r' mode opens the file for reading purpose only.	
		pageFile = fopen(fHandle->fileName, "r");
		
		//Checks for file if it is opened successfully or not.
		if(pageFile == NULL)
			return RC_FILE_NOT_FOUND;

		//File pointer position is initialised.
		fseek(pageFile, startPosition, SEEK_SET);
		
		int i;
		//Block is read character by character and storing it in memPage
		for(i = 0; i < PAGE_SIZE; i++) {
			memPage[i] = fgetc(pageFile);
		}

		//The current page position is set to the pointer position of the file stream
		fHandle->curPagePos = ftell(pageFile); 

		//All the buffers are flushed after closing file stream.
		fclose(pageFile);
		return RC_OK;
	}
}

extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//Finding the current page number by dividing current page position by size of the page. It will have position in bytes	
	int currentPageNumber = fHandle->curPagePos / PAGE_SIZE;
	int startPosition = (PAGE_SIZE * (currentPageNumber - 2));
	
	//'r' mode opens the file for reading purpose only.	
	pageFile = fopen(fHandle->fileName, "r");

	//Checks for file if it was successfully opened or not.
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND;

	
	fseek(pageFile, startPosition, SEEK_SET);
	
	int i;
	//Block is read character by character and stored in memPage.
	//Also checks if we have reahed end of file.
	for(i = 0; i < PAGE_SIZE; i++) {
		char c = fgetc(pageFile);		
		if(feof(pageFile))
			break;
		memPage[i] = c;
	}
	
	//Current page position is set to the pointer position of the file stream
	fHandle->curPagePos = ftell(pageFile); 

	// All the buffers are flushed.
	fclose(pageFile);
	return RC_OK;		
}

extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	//There's no next block to read so checks if we are on the last block.
	if(fHandle->curPagePos == PAGE_SIZE) {
		printf("\n Last block: Next block not present.");
		return RC_READ_NON_EXISTING_PAGE;	
	} else {
		//Finding the current page number by dividing current page position by size of the page. It will have position in bytes
		int currentPageNumber = fHandle->curPagePos / PAGE_SIZE;
		int startPosition = (PAGE_SIZE * (currentPageNumber - 2));

		//'r' mode opens the file for reading purpose only.	
		pageFile = fopen(fHandle->fileName, "r");
		
		//If the file was successfully opened or not is checked.
		if(pageFile == NULL)
			return RC_FILE_NOT_FOUND;
		
		
		fseek(pageFile, startPosition, SEEK_SET);
		
		int i;
		//Block is read character by character and memPage is used to store it.
		//If end of file is reached or not, is checked
		for(i = 0; i < PAGE_SIZE; i++) {
			char c = fgetc(pageFile);		
			if(feof(pageFile))
				break;
			memPage[i] = c;
		}

		
		fHandle->curPagePos = ftell(pageFile); 

		//All the buffers are flushed closing file stream.
		fclose(pageFile);
		return RC_OK;
	}
}

extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	//'r' mode opens the file for reading purpose only.	
	pageFile = fopen(fHandle->fileName, "r");

	//If file was successfully opened or not is checked.
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND;
	
	int startPosition = (fHandle->totalNumPages - 1) * PAGE_SIZE;

	
	fseek(pageFile, startPosition, SEEK_SET);
	
	int i;
	//Block is read character by character and memPage is used to store it.
	//If end of file is reached or not, is checked
	for(i = 0; i < PAGE_SIZE; i++) {
		char c = fgetc(pageFile);		
		if(feof(pageFile))
			break;
		memPage[i] = c;
	}
	
	fHandle->curPagePos = ftell(pageFile); 

	//All buffers are flushed closing file stream.
	fclose(pageFile);
	return RC_OK;	
}

extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//First check if the no of pages is proper or not
	if (pageNum > fHandle->totalNumPages || pageNum < 0)
        	return RC_WRITE_FAILED;
	
	//Open the file in write mode
	pageFile = fopen(fHandle->fileName, "r+");
		
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND;

	int startPosition = pageNum * PAGE_SIZE;

	if(pageNum == 0) { 
		//Writing data to non-first page
		fseek(pageFile, startPosition, SEEK_SET);	
		int i;
		for(i = 0; i < PAGE_SIZE; i++) 
		{
			//If it es end of file is not, is checked. If yes then append empty block.
			if(feof(pageFile)) 
				 appendEmptyBlock(fHandle);
			//memPage to page file character writing			
			fputc(memPage[i], pageFile);
		}

		//Current page position to the pointer position of the file stream is set
		fHandle->curPagePos = ftell(pageFile); 

		//All the buffers are flushed closing file stream.
		fclose(pageFile);	
	} else {	
		//Data is written to the first page.
		fHandle->curPagePos = startPosition;
		fclose(pageFile);
		writeCurrentBlock(fHandle, memPage);
	}
	return RC_OK;
}

//Write the block/page to the file on disk
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//'r+' mode opens the file for both reading and writing.	
	pageFile = fopen(fHandle->fileName, "r+");

	//If the file is successfully opened or not, is checked.
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND;
	
	//Empty block is appended to make some space for the new content.
	appendEmptyBlock(fHandle);

	//File pointer is initiliazed
	fseek(pageFile, fHandle->curPagePos, SEEK_SET);
	
	//memPage contents are written to the file.
	fwrite(memPage, sizeof(char), strlen(memPage), pageFile);
	
	//Current page position to the pointer position of the file stream is set
	fHandle->curPagePos = ftell(pageFile);

	//All the buffers are flushed closing file stream.     	
	fclose(pageFile);
	return RC_OK;
}


extern RC appendEmptyBlock (SM_FileHandle *fHandle) {
	//Empty page of size PAGE_SIZE is created
	SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
	
	//Cursor position is moved to the begining of the file stream.
	int isSeekSuccess = fseek(pageFile, 0, SEEK_END);
	
	if( isSeekSuccess == 0 ) {
		fwrite(emptyBlock, sizeof(char), PAGE_SIZE, pageFile);
	} else {
		free(emptyBlock);
		return RC_WRITE_FAILED;
	}
	
	//Previously allocated to 'emptyPage' memory is deallocated.	
	free(emptyBlock);
	
	//Total number of pages are incremented since we added an empty block.
	fHandle->totalNumPages++;
	return RC_OK;
}

extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
	//'a' opens file in append mode and add the data at the end of file.
	pageFile = fopen(fHandle->fileName, "a");
	
	if(pageFile == NULL)
		return RC_FILE_NOT_FOUND;
	
	//If numberOfPages is greater than totalNumPage then add empty pages until numberofPages = totalNumPages.
	while(numberOfPages > fHandle->totalNumPages)
		appendEmptyBlock(fHandle);
	
	//All the buffers are flushed closing file stream. 
	fclose(pageFile);
	return RC_OK;
}