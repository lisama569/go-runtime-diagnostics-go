package obs

import "context"

type bundleKey struct{}

func withBundle(ctx context.Context, b *Bundle) context.Context {
	return context.WithValue(ctx, bundleKey{}, b)
}

// FromContext returns the Bundle carried by ctx, if any.
func FromContext(ctx context.Context) (*Bundle, bool) {
	v := ctx.Value(bundleKey{})
	if v == nil {
		return nil, false
	}
	b, ok := v.(*Bundle)
	return b, ok
}
