/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package backtype.storm.tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 属性名称集合，本质上就是一个 List
 */
public class Fields implements Iterable<String>, Serializable {

    private static final long serialVersionUID = -5756089543121005742L;

    private List<String> _fields;

    private Map<String, Integer> _index = new HashMap<>();

    public Fields(String... fields) {
        this(Arrays.asList(fields));
    }

    /**
     * 构造一个属性集合，属性名称不允许重复
     *
     * @param fields
     */
    public Fields(List<String> fields) {
        _fields = new ArrayList<>(fields.size());
        for (String field : fields) {
            if (_fields.contains(field)) {
                throw new IllegalArgumentException(String.format("duplicate field '%s'", field));
            }
            _fields.add(field);
        }
        // 记录各个 <属性名称, 下标> 到 _index 中
        this.index();
    }

    /**
     * 从 tuple 中选择指定属性的值返回
     *
     * @param selector
     * @param tuple
     * @return
     */
    public List<Object> select(Fields selector, List<Object> tuple) {
        List<Object> ret = new ArrayList<>(selector.size());
        for (String s : selector) {
            ret.add(tuple.get(_index.get(s)));
        }
        return ret;
    }

    /**
     * 返回指定属性名称对应的值
     *
     * @param selector
     * @param tuple
     * @return
     */
    public Object select(String selector, List<Object> tuple) {
        return tuple.get(_index.get(selector));
    }

    public List<String> toList() {
        return new ArrayList<>(_fields);
    }

    public int size() {
        return _fields.size();
    }

    public String get(int index) {
        return _fields.get(index);
    }

    public Iterator<String> iterator() {
        return _fields.iterator();
    }

    /**
     * 获取属性对应的下标
     */
    public int fieldIndex(String field) {
        Integer ret = _index.get(field);
        if (ret == null) {
            throw new IllegalArgumentException(field + " does not exist");
        }
        return ret;
    }

    /**
     * 判断属性是否存在
     */
    public boolean contains(String field) {
        return _index.containsKey(field);
    }

    /**
     * 记录各个属性对应的 index
     */
    private void index() {
        for (int i = 0; i < _fields.size(); i++) {
            _index.put(_fields.get(i), i);
        }
    }

    @Override
    public String toString() {
        return _fields.toString();
    }
}
