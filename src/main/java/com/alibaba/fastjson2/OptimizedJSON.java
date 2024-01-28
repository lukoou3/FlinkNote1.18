package com.alibaba.fastjson2;

import java.util.Map;

import static com.alibaba.fastjson2.JSONReader.EOI;
import static com.alibaba.fastjson2.JSONReader.Feature.IgnoreCheckClose;

public class OptimizedJSON {

    public static void parseObject(String text, Map object) {
        if (text == null || text.isEmpty()) {
            return;
        }

        final JSONReader.Context context = JSONFactory.createReadContext();
        try (JSONReader reader = JSONReader.of(text, context)) {
            if (reader.nextIfNull()) {
                return;
            }
            reader.read(object, 0L);
            if (reader.resolveTasks != null) {
                reader.handleResolveTasks(object);
            }
            if (reader.ch != EOI && (context.features & IgnoreCheckClose.mask) == 0) {
                throw new JSONException(reader.info("input not end"));
            }
            return;
        }
    }
}
