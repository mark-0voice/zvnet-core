#include <lua.h>
#include <stdio.h>
#include "zvmalloc.h"
#include <luajit.h>
#include <lualib.h>
#include <lauxlib.h>

#ifdef __linux__
    #include <sys/sendfile.h>
#endif


typedef struct {
    size_t mem;
    size_t mem_level;
} lstate_t;

const size_t MEMLVL = 2097152; // 2M
const size_t MEM_1MB = 1048576; // 1M

static int
traceback (lua_State *L) {
    const char *msg = lua_tostring(L, 1);
    if (msg)
        luaL_traceback(L, L, msg, 1);
    else {
        lua_pushliteral(L, "no error message");
    }
    return 1;
}

void *
lua_alloc(void *ud, void *ptr, size_t osize, size_t nsize) {
    lstate_t *s = (lstate_t*)ud;
    s->mem += nsize;
    if (ptr) s->mem -= osize;
    if (s->mem > s->mem_level) {
        do {
            s->mem_level += MEMLVL;
        } while (s->mem > s->mem_level);
        
        printf("luajit vm now use %.2f M's memory up\n", (float)s->mem / MEM_1MB);
    } else if (s->mem < s->mem_level - MEMLVL) {
        do {
            s->mem_level -= MEMLVL;
        } while (s->mem < s->mem_level);
        
        printf("luajit vm now use %.2f M's memory down\n", (float)s->mem / MEM_1MB);
    }
    if (nsize == 0) {
        free(ptr);
        return NULL;
    } else {
        return realloc(ptr, nsize);
    }
}

static int
lsendfile(lua_State *L) {
#ifdef __linux__
    FILE** f = (FILE**)luaL_checkudata(L,1,"FILE*");
    if (*f == NULL)
        luaL_error(L, "attempt to use a closed file");
    int fd = fileno(*f);
    int outfd = luaL_checkinteger(L, 2);
    // off_t *offset = (off_t*)luaL_checkinteger(L, 3);
    size_t count = luaL_checkinteger(L, 3);
    off_t offset = 0;
    ssize_t n = sendfile(outfd, fd, &offset, count);
    printf("sendfile fd:%d outfd:%d count:%lu n:%ld\n", fd, outfd, count, n);
#else
    return luaL_error(L, "not support sendfile at this system");
#endif
    lua_pushinteger(L, n);
    return 1;
}

static void
lua_inject_api(lua_State *L) {
    // add null
    lua_pushlightuserdata(L, NULL);
    lua_setglobal(L, "null");

#ifdef __linux__
    lua_pushcfunction(L, lsendfile);
    lua_setglobal(L, "sendfile");
#endif
}

int main(int argc, char** argv) {
    lstate_t ud = {0, MEMLVL};
    lua_State *L = lua_newstate(lua_alloc, &ud);
    luaL_openlibs(L);
    lua_inject_api(L);
    if (argc > 1) {
        lua_pushcfunction(L, traceback);
        int r = luaL_loadfile(L, argv[1]);
        if (LUA_OK != r) {
            const char* err = lua_tostring(L, -1);
            fprintf(stderr, "can't load %s err:%s\n", argv[1], err);
            return 1;
        }
        r = lua_pcall(L, 0, LUA_MULTRET, 1);
        if (LUA_OK != r) {
            const char* err = lua_tostring(L, -1);
            fprintf(stderr, "lua file %s launch err:%s\n", argv[1], err);
            return 1;
        }
    } else {
        fprintf(stderr, "please provide main lua file\n");
    }
    return 0;
}
