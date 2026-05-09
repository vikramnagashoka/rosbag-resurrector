// CSS module type declarations — every *.module.css import resolves to
// a record of class-name strings.
declare module '*.module.css' {
  const classes: { readonly [key: string]: string }
  export default classes
}
