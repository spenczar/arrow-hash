use arrow::array;
use arrow::array::Array;
use arrow::datatypes;
use arrow::pyarrow::{ToPyArrow, FromPyArrow};
use hashbrown;
use pyo3::prelude::*;


#[pyclass]
struct ArrowUInt8Index {
    hashtable: hashbrown::HashMap<u8, array::UInt64Array>,
}

#[pyclass]
struct ArrowUInt16Index {
    hashtable: hashbrown::HashMap<u16, array::UInt64Array>,
}

#[pyclass]
struct ArrowUInt32Index {
    hashtable: hashbrown::HashMap<u32, array::UInt64Array>,
}

#[pyclass]
struct ArrowUInt64Index {
    hashtable: hashbrown::HashMap<u64, array::UInt64Array>,
}

#[pyclass]
struct ArrowInt8Index {
	hashtable: hashbrown::HashMap<i8, array::UInt64Array>,
}

#[pyclass]
struct ArrowInt16Index {
	hashtable: hashbrown::HashMap<i16, array::UInt64Array>,
}

#[pyclass]
struct ArrowInt32Index {
	hashtable: hashbrown::HashMap<i32, array::UInt64Array>,
}

#[pyclass]
struct ArrowInt64Index {
	hashtable: hashbrown::HashMap<i64, array::UInt64Array>,
}

// This code uses macros because pyclass doesn't support generics.
macro_rules! index_for_primitive {
    ($struct_name:ident, $arrow_type: ty, $rust_type: ty) => {
        impl From<array::PrimitiveArray<$arrow_type>> for $struct_name {
            fn from(data: array::PrimitiveArray<$arrow_type>) -> Self {
                let mut builder_table = hashbrown::HashMap::with_capacity(data.len());
                for i in 0..data.len() {
                    let builder = builder_table.entry(data.value(i)).or_insert(array::UInt64Builder::new());
		    builder.append_value(i as u64);
                }

		let mut hashtable = hashbrown::HashMap::with_capacity(builder_table.len());
		for (key, mut builder) in builder_table {
		    hashtable.insert(key, builder.finish());
		}
		Self { hashtable }
            }
        }

        impl $struct_name {
            fn get(
                &self,
                key: $rust_type,
            ) -> Option<&array::UInt64Array> {
                self.hashtable.get(&key)
            }
        }
    };
}

index_for_primitive!(ArrowUInt8Index, datatypes::UInt8Type, u8);
index_for_primitive!(ArrowUInt16Index, datatypes::UInt16Type, u16);
index_for_primitive!(ArrowUInt32Index, datatypes::UInt32Type, u32);
index_for_primitive!(ArrowUInt64Index, datatypes::UInt64Type, u64);

index_for_primitive!(ArrowInt8Index, datatypes::Int8Type, i8);
index_for_primitive!(ArrowInt16Index, datatypes::Int16Type, i16);
index_for_primitive!(ArrowInt32Index, datatypes::Int32Type, i32);
index_for_primitive!(ArrowInt64Index, datatypes::Int64Type, i64);

macro_rules! test_primitive_index {
    ($struct_name:ident, $arrow_type: ty, $rust_type: ty) => {
	{
	    let array = array::PrimitiveArray::<$arrow_type>::from(vec![1, 2, 3, 3, 5]);
	    let idx = $struct_name::from(array);
	    let have = idx.get(1 as $rust_type);
	    let want = array::UInt64Array::from(vec![0]);
	    assert_eq!(*have.unwrap(), want);
	}
    };
}
#[test]
fn test_build_primitive_index() {
    test_primitive_index!(ArrowUInt8Index, datatypes::UInt8Type, u8);
    test_primitive_index!(ArrowUInt16Index, datatypes::UInt16Type, u16);
    test_primitive_index!(ArrowUInt32Index, datatypes::UInt32Type, u32);
    test_primitive_index!(ArrowUInt64Index, datatypes::UInt64Type, u64);
    test_primitive_index!(ArrowInt8Index, datatypes::Int8Type, i8);
    test_primitive_index!(ArrowInt16Index, datatypes::Int16Type, i16);
    test_primitive_index!(ArrowInt32Index, datatypes::Int32Type, i32);
    test_primitive_index!(ArrowInt64Index, datatypes::Int64Type, i64);
}

#[pymethods]
impl ArrowInt64Index {
    #[pyo3(name="get")]
    fn get_py(&self, key: i64, py: Python<'_>) -> PyResult<Option<PyObject>> {
	let array = self.get(key);
	if array.is_none() {
	    return Ok(None);
	}
	let array = array.unwrap();
	let array_data: array::ArrayData = array.clone().into();

	let pyarray = array_data.to_pyarrow(py)?;
	Ok(Some(pyarray))

    }

    #[new]
    fn py_new(data: &PyAny) -> PyResult<Self> {
	let array_data = array::ArrayData::from_pyarrow(data)?;
	if array_data.data_type() != &datatypes::DataType::Int64 {
	    return Err(pyo3::exceptions::PyTypeError::new_err("Expected Int64Type"));
	}
	let array = array::Int64Array::from(array_data);
	let idx = Self::from(array);
	Ok(idx)
    }
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: u64, b: u64) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn arrow_hash(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_class::<ArrowInt64Index>()?;
    Ok(())
}
