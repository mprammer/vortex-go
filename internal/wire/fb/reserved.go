// SPDX-License-Identifier: Apache-2.0

package fb

// HasTransforms exposes reserved schema fields whose generated Go names begin
// with an underscore and therefore cannot be accessed outside this package.
func (s *PostscriptSegment) HasTransforms() bool {
	return s._Compression(nil) != nil || s._Encryption(nil) != nil
}
func (s *SegmentSpec) HasTransforms() bool { return s._Compression() != 0 || s._Encryption() != 0 }
