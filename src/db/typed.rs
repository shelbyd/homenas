use serde::{de::DeserializeOwned, Serialize};

use super::*;

#[async_trait::async_trait]
pub trait CborTypedExt {
    async fn set_typed<T>(&self, key: &str, t: Option<&T>) -> IoResult<()>
    where
        T: Serialize + Sync;

    async fn get_typed<T>(&self, key: &str) -> IoResult<Option<T>>
    where
        T: DeserializeOwned;
}

#[async_trait::async_trait]
impl<D> CborTypedExt for D
where
    D: Tree + Sized,
{
    async fn set_typed<T>(&self, key: &str, t: Option<&T>) -> IoResult<()>
    where
        T: Serialize + Sync,
    {
        match t {
            None => self.set(key, None).await,
            Some(t) => self.set(key, Some(&ser(t)?)).await,
        }
    }

    async fn get_typed<T>(&self, key: &str) -> IoResult<Option<T>>
    where
        T: DeserializeOwned,
    {
        match self.get(key).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(de(&bytes)?)),
        }
    }
}

fn ser<T: Serialize>(t: &T) -> IoResult<Vec<u8>> {
    serde_cbor::to_vec::<T>(t).map_err(|e| {
        log::error!("Error serializing stored data: {}", e);
        IoError::InvalidData
    })
}

fn de<T: DeserializeOwned>(bytes: impl AsRef<[u8]>) -> IoResult<T> {
    serde_cbor::from_slice::<T>(bytes.as_ref()).map_err(|e| {
        log::error!("Error deserializing stored data: {}", e);
        IoError::InvalidData
    })
}

/// Update the value at the provided key. May retry until successful.
pub async fn update_typed<V, R, F, T>(tree: &T, key: &str, mut f: F) -> IoResult<R>
where
    T: Tree,
    F: for<'v> FnMut(Option<V>) -> IoResult<(Option<V>, R)> + Send,
    V: Serialize + DeserializeOwned,
    R: Send,
{
    let mut current_bytes = tree.get(key).await?;

    loop {
        let current = current_bytes
            .as_ref()
            .map(|bytes| de::<V>(bytes))
            .transpose()?;
        let (next, r) = f(current)?;

        let next_bytes: Option<Vec<u8>> = next.map(|v| ser(&v)).transpose()?;
        let swap_result = tree
            .compare_and_swap(key, current_bytes.as_deref(), next_bytes.as_deref())
            .await?;

        match swap_result {
            Ok(()) => return Ok(r),
            Err(swap_err) => {
                current_bytes = swap_err.current;
            }
        }
    }
}
