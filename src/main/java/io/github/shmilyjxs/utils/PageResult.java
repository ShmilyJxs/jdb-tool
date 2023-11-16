package io.github.shmilyjxs.utils;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PageResult<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long pageNum;

    private final long pageSize;

    private final long total;

    private final long pages;

    private final List<T> records;

    public PageResult(long pageNum, long pageSize, long total, long pages, List<T> records) {
        this.pageNum = pageNum;
        this.pageSize = pageSize;
        this.total = total;
        this.pages = pages;
        this.records = Optional.ofNullable(records).orElse(Collections.emptyList());
    }

    public static <T> PageResult<T> of(long pageNum, long pageSize, long total, long pages, List<T> records) {
        return new PageResult<>(pageNum, pageSize, total, pages, records);
    }

    public <R> PageResult<R> map(Function<T, R> mapper) {
        return PageResult.of(pageNum, pageSize, total, pages, records.stream().map(mapper).collect(Collectors.toList()));
    }

    public long getPageNum() {
        return pageNum;
    }

    public long getPageSize() {
        return pageSize;
    }

    public long getTotal() {
        return total;
    }

    public long getPages() {
        return pages;
    }

    public List<T> getRecords() {
        return records;
    }
}