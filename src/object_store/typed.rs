use serde::{de::DeserializeOwned, Serialize};

use super::*;

#[async_trait::async_trait]
pub trait CborTypedExt {
    async fn set_typed<T>(&self, key: &str, t: &T) -> IoResult<()>
    where
        T: Serialize + Sync;

    async fn get_typed<T>(&self, key: &str) -> IoResult<T>
    where
        T: DeserializeOwned;
}

#[async_trait::async_trait]
impl<O> CborTypedExt for O
where
    O: ObjectStore + Sized,
{
    async fn set_typed<T>(&self, key: &str, t: &T) -> IoResult<()>
    where
        T: Serialize + Sync,
    {
        self.set(key, &ser(t)?).await
    }

    async fn get_typed<T>(&self, key: &str) -> IoResult<T>
    where
        T: DeserializeOwned,
    {
        de(&self.get(key).await?)
    }
}

fn ser<T: Serialize>(t: &T) -> IoResult<Vec<u8>> {
    serde_cbor::to_vec::<T>(t).map_err(|e| {
        log::error!("Error serializing stored data: {}", e);
        IoError::InvalidData
    })
}

fn de<T: DeserializeOwned>(bytes: &[u8]) -> IoResult<T> {
    serde_cbor::from_slice::<T>(bytes).map_err(|e| {
        log::error!("Error deserializing stored data: {}", e);
        IoError::InvalidData
    })
}

/// Update the value at the provided key. May retry until successful.
pub async fn update_typed<T, R, F, O>(store: &O, key: &str, mut f: F) -> IoResult<R>
where
    O: ObjectStore,
    F: for<'v> FnMut(Option<T>) -> IoResult<(T, R)> + Send,
    T: Serialize + DeserializeOwned,
    R: Send,
{
    super::update(store, key, |read| {
        let t = read.map(|r| de(r)).transpose()?;
        let (new_t, r) = f(t)?;
        let bytes = ser(&new_t)?;
        Ok((bytes, r))
    })
    .await
}
