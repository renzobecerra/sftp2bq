Both the host and the client should have the following permissions and owners:

- ~./ssh permissions should be 700
- ~./ssh should be owned by your account
- ~/.ssh/authorized_keys permissions should be 600
- ~/.ssh/authorized_keys should be owned by your account


Client environments should additionally have the following permissions and owners:

- ~/.ssh/config permissions should be 600
- ~/.ssh/id_* permissions should be 600