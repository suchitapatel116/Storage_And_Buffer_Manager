CC := gcc
SRC := dberror.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c test_assign2_1.c
OBJ := dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o test_assign2_1.o
assignment1: $(OBJ)
	$(CC) -o test_assign1 dberror.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c test_assign2_1.c
%.o: %.c
	$(CC) -g -c $<
run: assignment1
	./test_assign1
clean:
	rm -rf test_assign1 *.o
