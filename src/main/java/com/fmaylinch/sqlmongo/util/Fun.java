package com.fmaylinch.sqlmongo.util;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Fun {

	/** Abbreviation for typical mapping to List */
	public static <T, U> List<U> map(Collection<T> col, Function<? super T, ? extends U> f) {
		return col.stream().map(f).collect(Collectors.toList());
	}
}
