use {
    crate::{IOFSErrorKind, UfsUuid},
    jsonwebtoken as jwt,
    jwt::{decode, encode, errors::ErrorKind, Algorithm, Header, Validation},
    log::error,
    serde::{Deserialize, Serialize},
};

pub(crate) type JWT = String;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct UserClaims {
    pub(crate) iss: UfsUuid,
    pub(crate) sub: UfsUuid,
    pub(crate) exp: usize,
}

pub(crate) fn new_jwt(claims: UserClaims, secret: &[u8]) -> JWT {
    encode(&Header::default(), &claims, secret).expect("unable to create JWT")
}

pub(crate) fn decode_jwt(token: JWT, secret: &String) -> Result<UserClaims, failure::Error> {
    match decode::<UserClaims>(&token, secret.as_bytes(), &Validation::default()) {
        Ok(decoded) => Ok(decoded.claims),
        Err(e) => match e.kind() {
            ErrorKind::InvalidToken => Err(IOFSErrorKind::InvalidToken.into()),
            ErrorKind::InvalidSignature => Err(IOFSErrorKind::InvalidSignature.into()),
            ErrorKind::ExpiredSignature => Err(IOFSErrorKind::TokenExpired.into()),
            e => {
                error!("Unexpected JWT validation error: {:?}", e);
                Err(IOFSErrorKind::InvalidToken.into())
            }
        },
    }
}

#[cfg(test)]
mod test {
    use {super::*, chrono::prelude::*, time::Duration};

    #[test]
    fn expired_token() {
        let exp = Utc::now() - Duration::seconds(5);
        let token = new_jwt(
            UserClaims {
                iss: UfsUuid::new_root_fs("foo"),
                sub: UfsUuid::new_user("foo"),
                exp: exp.timestamp() as usize,
            },
            "secret".as_bytes(),
        );

        match decode_jwt(token, &"secret".to_string()) {
            Ok(_) => assert!(false, "token should be expired"),
            Err(_) => assert!(true, "token was expired"),
        }
    }
}
