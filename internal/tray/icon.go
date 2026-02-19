package tray

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"image"
	"image/color"
	"log"
	"math"
)

// trayIconBase64 is the shared ICO (32x32) used for both system tray and GUI favicon.
const trayIconBase64 = "" +
	"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////////9u/p/7h9Wf/kfmf/5odx//Lk2//59vH/5pJ8/71zT/+8ck7/oGo+/5BmNP+QZjT/0Hhb/+PBrZPkrpjZu3JO/pBmNP+gaz3/5Ipz/+izn//ps6D/6cCt//Dg1f/miXT/5X5n/+m2o//+/fv///////////////////////Ho3//gm4T/5X5n/+ebhv/48+3/6sOx/+i4pf/os5//5o14/6ZtQv+QZjT/u3JO/+Wwmtblo43pu3hT/ZBmNP/HdlX/6sSy/////////////Pr4/+ihjf/lfmf/56ON//v49f//////+ff1///////////////////////16+T/55B7/+V+Z//nr5v//fz7////////////7dHC/8l2Vv+QZjT/sm9I/+Skj+PloYnzwoNh/pBmNP/WeV3/9Ofc////////////8NnM/+WAaf/ninT/9Onf///////59/X/lm4//+ng1v/39PH/nnlN/+7n3///////7c29/+V/aP/mg23/8d/U////////////8+jf/9N5XP+QZjT/qW5D/+ahi+bloozvyIhp/5BmNP/YemD/9Ovi////////////6bKf/+V+Z//nrZj//v79///////LuKD/kGc1//Ls5v/7+ff/kWc2/8CojP///////Pn3/+ifiv/lfmf/6K6a////////////8+ng/9Z6X/+QZjT/rG5F/+WgienlqZPj1pZ7/5BmNP/TeV3/8+jd/////////v7/56SP/+V+Z//rxbX/w62S/6F9Uv+QZzX/v6eL////////////uJ5+/5BnNf+2mnn/zLih/+e5p//lfmf/55mF//v59v//////8uLX/9B4Wv+QZjT/snBJ/+WnkuPktqG95KSM/pZnOP7IdlX/7dDB///////8+vf/55qG/+V+Z//t0cL/0L+q/8Kqj//h1sj/////////////////+vj2/7idfP+SaDf/kWc2/+DFsv/lfmf/55F9//n28f//////7dDA/8R1U/+QZjT/vHJP/+Svm9PjybF/5ZB7/ap0SvisbkX/55qG//Dc0f/t08T/5414/+V+Z//nl4P/56GM/+eolP/puaf/8N7T//z59//////////////////49fL/9vLu/+/ZzP/lfmf/6JN///r38///////6Lak/7JwSf+QZjT/0Hha/+TCq4/m0bw95Y53/cmXeP2QZjT/unJN/9Z6Xv/UeV3/y3dY/8R1U/+8c0//v3RQ/8p2V//gfWT/5YBp/+ehjP/x4NX//////////////////////+/Uxv/lfmf/56OO//79/f/48uz/5pN//5loOf+SZjb/4YRr8OXSv0T/qqoD5aaQ7eOtlv6XaDj+kGY0/5BmNP+QZjT/kGY0/5BmNP+QZjT/kGY0/5BmNP+TZjb/tnFL/+F9Zf/mi3X/8NrN/////////////////+nArf/lfmf/55eC/+iynv/nmIP/vHNP/5BmNP+ubkb/5qKM59vbtgcAAAAA5sCsmuWSe/zAjWv5kWY1/5BmNP+QZjT/kGY0/5BmNP+QZjT/kGY0/5BmNP+QZjT/kGY0/59rPv/efGP/55aA//n07///////+/j1/+ieiv/lfmf/5aKM/9OSdv+2e1f/n21A/5lpOv/QeFv/5sCpoQAAAAAAAAAA5dG+J+aWgPvlq5T727GY/8uggf7EknL9xJJy/cWWc/7QpIf/xpZ2/5lqO/+QZjT/kGY0/5BmNP+nbUH/5X5n/+vKuv/05t3/56mV/+V/aP/mkHr/8+rg//n07//17+f/7dzO/+asl//mln/559W+KwAAAAAAAAAAAAAAAObFs27mp5Hn5ZF6+OaIcPzmiXP75Ipy+uaHcPvmiXH85o93/uK4of/CknH/qHRJ/5dpOf+SZzX/0n9i/+aOeP/lg23/5YBo/+aQevTlhnD55Yly/eWPeP3mkHn+5ZB5/uaplPDlxbBqAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA5dG+J+bTvlLkzLVy5MeyeubNuHDj0r5K5bmkueWQe/nlkHn85qiT/eWznv/ltJ//5pmF/eV+Z//mkHrz576ovOjRvyzl1b5O5dO+YuTMunLm079c5tK+M/+qqgMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAObSvjPkwKqi5auV3uSfie7mooro5aiS4OS8pqvm0b0yAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/+AP8//AD/PhwA/z4YBI88CAcPPAAGDzwABg88AAAfPgAAHz4AAB84AAADIAAAASAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAEgAAABMAAAAzwAAAc/+Af/A=="

// FaviconDataURI returns the tray icon as a data URI for use in HTML (e.g. <link rel="icon" href="...">).
// Same ICO as the system tray; browsers support ICO for favicons.
func FaviconDataURI() string {
	return "data:image/x-icon;base64," + trayIconBase64
}

func generateIconBase64() []byte {
	data, err := base64.StdEncoding.DecodeString(trayIconBase64)
	if err != nil {
		log.Printf("failed to decode tray icon: %v", err)
		return nil
	}
	return data
}

func generateIcon() []byte {
	const size = 32
	img := image.NewRGBA(image.Rect(0, 0, size, size))

	// Colors inspired by PostgreSQL logo
	pgBlue := color.RGBA{R: 0x33, G: 0x67, B: 0x91, A: 0xFF} // #336791
	white := color.RGBA{R: 0xFF, G: 0xFF, B: 0xFF, A: 0xFF}

	// Transparent background
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			img.Set(x, y, color.RGBA{0, 0, 0, 0})
		}
	}

	// Blue circle background
	cx, cy := float64(size)/2, float64(size)/2
	r := float64(size)/2 - 1
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			dx := float64(x) - cx
			dy := float64(y) - cy
			if dx*dx+dy*dy <= r*r {
				img.Set(x, y, pgBlue)
			}
		}
	}

	// Very simple cylinder suggestion (two thin white arcs) near the bottom,
	// evoking a database shape / elephant head arc, without overcomplicating.
	drawArc(img, cx, cy+3, r-6, white)
	drawArc(img, cx, cy+5, r-7, white)

	// Draw blocky \"P\" and \"T\" letters in white, centered.
	// Coordinates tuned by eye for size=32.
	drawLetterP(img, 9, 9, 6, 14, white)
	drawLetterT(img, 18, 9, 6, 14, white)

	return encodeICOFromRGBA(img)
}

func drawArc(img *image.RGBA, cx, cy float64, radius float64, c color.RGBA) {
	if radius <= 0 {
		return
	}
	for deg := 210.0; deg <= 330.0; deg += 2 {
		angle := deg * math.Pi / 180.0
		x := int(math.Round(cx + radius*math.Cos(angle)))
		y := int(math.Round(cy + radius*math.Sin(angle)))
		if image.Pt(x, y).In(img.Bounds()) {
			img.Set(x, y, c)
		}
	}
}

// drawLetterP draws a block-style \"P\" in the given rectangle.
func drawLetterP(img *image.RGBA, x, y, w, h int, c color.RGBA) {
	// Vertical stem
	for yy := y; yy < y+h; yy++ {
		for xx := x; xx < x+2; xx++ {
			img.Set(xx, yy, c)
		}
	}
	// Top bar
	for yy := y; yy < y+3; yy++ {
		for xx := x; xx < x+w; xx++ {
			img.Set(xx, yy, c)
		}
	}
	// Middle bar
	midY := y + h/2
	for yy := midY - 1; yy <= midY+1; yy++ {
		for xx := x; xx < x+w; xx++ {
			img.Set(xx, yy, c)
		}
	}
	// Right side of the upper loop
	for yy := y + 2; yy < midY; yy++ {
		for xx := x + w - 2; xx < x+w; xx++ {
			img.Set(xx, yy, c)
		}
	}
}

// drawLetterT draws a block-style \"T\" in the given rectangle.
func drawLetterT(img *image.RGBA, x, y, w, h int, c color.RGBA) {
	// Top bar
	for yy := y; yy < y+3; yy++ {
		for xx := x; xx < x+w; xx++ {
			img.Set(xx, yy, c)
		}
	}
	// Vertical stem
	centerX := x + w/2
	for yy := y + 3; yy < y+h; yy++ {
		for xx := centerX - 1; xx <= centerX+1; xx++ {
			img.Set(xx, yy, c)
		}
	}
}

// encodeICOFromRGBA builds a minimal 32x32 32-bit .ico file from an RGBA image.
// This is enough for Windows tray icons and is accepted by systray.SetIcon.
func encodeICOFromRGBA(img *image.RGBA) []byte {
	const (
		icoHeaderSize = 6  // ICONDIR
		icoEntrySize  = 16 // ICONDIRENTRY
		bmpHeaderSize = 40 // BITMAPINFOHEADER
	)

	w := img.Bounds().Dx()
	h := img.Bounds().Dy()

	// XOR bitmap: 32-bit BGRA, bottom-up
	xorSize := w * h * 4
	// AND mask: 1 bit per pixel, rows padded to 32 bits (4 bytes)
	andRowBytes := ((w + 31) / 32) * 4
	andSize := andRowBytes * h
	imageDataSize := bmpHeaderSize + xorSize + andSize

	var buf bytes.Buffer

	// ICONDIR
	_ = binary.Write(&buf, binary.LittleEndian, uint16(0)) // Reserved
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1)) // Type = icon
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1)) // Count = 1

	// ICONDIRENTRY
	buf.WriteByte(byte(w))                                  // Width
	buf.WriteByte(byte(h))                                  // Height
	buf.WriteByte(0)                                        // Color count
	buf.WriteByte(0)                                        // Reserved
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))  // Planes
	_ = binary.Write(&buf, binary.LittleEndian, uint16(32)) // BitCount
	_ = binary.Write(&buf, binary.LittleEndian, uint32(imageDataSize))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(icoHeaderSize+icoEntrySize))

	// BITMAPINFOHEADER
	_ = binary.Write(&buf, binary.LittleEndian, uint32(bmpHeaderSize))
	_ = binary.Write(&buf, binary.LittleEndian, int32(w))
	_ = binary.Write(&buf, binary.LittleEndian, int32(h*2)) // height *2 (XOR+AND)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))  // Planes
	_ = binary.Write(&buf, binary.LittleEndian, uint16(32)) // BitCount
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0))  // Compression (BI_RGB)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(xorSize+andSize))
	_ = binary.Write(&buf, binary.LittleEndian, int32(0))  // XPelsPerMeter
	_ = binary.Write(&buf, binary.LittleEndian, int32(0))  // YPelsPerMeter
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0)) // ClrUsed
	_ = binary.Write(&buf, binary.LittleEndian, uint32(0)) // ClrImportant

	// XOR mask (pixel data), bottom-up, BGRA
	for y := h - 1; y >= 0; y-- {
		for x := 0; x < w; x++ {
			r, g, b, a := img.At(x, y).RGBA()
			br := byte(b >> 8)
			bg := byte(g >> 8)
			bb := byte(r >> 8)
			ba := byte(a >> 8)
			buf.WriteByte(br)
			buf.WriteByte(bg)
			buf.WriteByte(bb)
			buf.WriteByte(ba)
		}
	}

	// AND mask: all zeros (no additional transparency mask; alpha is in XOR)
	for y := 0; y < h; y++ {
		for x := 0; x < andRowBytes; x++ {
			buf.WriteByte(0)
		}
	}

	return buf.Bytes()
}
