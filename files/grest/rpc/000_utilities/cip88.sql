
CREATE OR REPLACE FUNCTION grest.safe_verify_cip88_pool_key_registration(bytes bytea)
RETURNS boolean
LANGUAGE plpgsql STABLE
AS $$
BEGIN
    RETURN cardano.tools_verify_cip88_pool_key_registration(bytes);
EXCEPTION WHEN OTHERS THEN
    RETURN false;
END;
$$;

COMMENT ON FUNCTION grest.safe_verify_cip88_pool_key_registration IS 'Wrapper around tools_verify_cip88_pool_key_registration that treats any errors as failed verification'; -- noqa: LT01
