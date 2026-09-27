package obs

import "errors"

// ErrAlreadyFinished is returned by a repeated Finish.
var ErrAlreadyFinished = errors.New("obs: bundle already finished")
