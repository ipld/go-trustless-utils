// Package traversal provides utilities that operate above the
// github.com/ipld/go-ipld-prime/traversal system to perform the kinds of
// traversals required by the IPFS Trustless Gateway protocol, which requires
// only limited subset of the full go-ipld-prime traversal system.
//
// Utilities are also provided to verify CAR streams against expected traversals
// and the LinkSystems used throughout can also be used to produce
// IPFS Trustless Gateway CAR streams when coupled with the
// github.com/ipld/go-car/v2/storage package.
package traversal
